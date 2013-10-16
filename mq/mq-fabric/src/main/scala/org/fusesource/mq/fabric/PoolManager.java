package org.fusesource.mq.fabric;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.fusesource.fabric.api.FabricException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component(name = "org.fusesource.mq.pool.manager")
@Service(PoolManager.class)
public class PoolManager {

    private final Multimap<String, ClusteredBroker> pools = Multimaps.synchronizedSetMultimap(HashMultimap.<String, ClusteredBroker>create());

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private BlockingQueue<PoolChangeEvent> events = new LinkedBlockingQueue();


    @Activate
    void activate() {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                mainLoop();
            }
        });
    }

    @Deactivate
    void deactivate() {
        //We need to wait for pending events.
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            FabricException.launderThrowable(e);
        }
    }

    public synchronized boolean canAcquire(ClusteredBroker manager) {
        String pool = manager.getBrokerConfiguration().getPool();
        if (pool == null) {
            return true;
        } else {
            return !pools.containsKey(pool);
        }
    }

    public synchronized boolean takeFromPool(ClusteredBroker manager) {
        String pool = manager.getBrokerConfiguration().getPool();
        if (pool == null) {
            return true;
        } else if (pools.containsKey(pool)){
            return false;
        } else {
            pools.put(pool, manager);
            firePoolChangeEvent(pool);
            return true;
        }
    }

    public synchronized void returnToPool(ClusteredBroker manager) {
        String pool = manager.getBrokerConfiguration().getPool();
        if (pool != null) {
            pools.removeAll(pool);
            firePoolChangeEvent(pool);
        }
    }

    public void firePoolChangeEvent(String pool) {
         events.add(new PoolChangeEvent(this, pool));
    }

    private void processPoolChangeEvent(String pool) throws Exception {
        for (ClusteredBroker configuration : pools.get(pool)) {
            configuration.updatePoolState();
        }
    }


    private void mainLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                events.take().invoke();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                FabricException.launderThrowable(e);
            }
        }
    }


    private class PoolChangeEvent  {
        private final PoolManager manager;
        private final String pool;

        private PoolChangeEvent(PoolManager manager, String pool) {
            this.manager = manager;
            this.pool = pool;
        }

        public void invoke() {
            try {
                manager.processPoolChangeEvent(pool);
            } catch (Exception e) {
                FabricException.launderThrowable(e);
            }
        }
    }
}
