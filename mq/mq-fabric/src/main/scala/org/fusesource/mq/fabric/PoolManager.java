package org.fusesource.mq.fabric;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.fusesource.fabric.api.FabricException;

import java.util.*;
import java.util.concurrent.*;

@Component(name = "org.fusesource.mq.pool.manager")
@Service(PoolManager.class)
public class PoolManager {

    private final ConcurrentHashMap<String, ClusteredBroker> slotOwners = new ConcurrentHashMap<String, ClusteredBroker>();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    List<ClusteredBroker> brokers = Collections.synchronizedList(new ArrayList<ClusteredBroker>());

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

    public synchronized boolean canAcquire(ClusteredBroker broker) {
        String pool = broker.getBrokerConfiguration().getPool();
        if (pool == null) {
            return true;
        } else {
            ClusteredBroker owner = slotOwners.get(pool);
            return owner==null || owner == broker;
        }
    }

    public synchronized boolean takeFromPool(ClusteredBroker broker) {
        String pool = broker.getBrokerConfiguration().getPool();
        if (pool == null) {
            return true;
        } else {
            ClusteredBroker owner = slotOwners.get(pool);
            if ( owner==broker ){
                return true; // we already took it.
            } else if ( owner!=null ){
                return false; // someone else took it.
            } else {
                // lets take it.
                slotOwners.put(pool, broker);
                firePoolChangeEvent();
                return true;
            }
        }
    }

    public synchronized void returnToPool(ClusteredBroker broker) {
        String pool = broker.getBrokerConfiguration().getPool();
        if (pool != null) {
            if( slotOwners.remove(pool, broker) ) {
                firePoolChangeEvent();
            }
        }
    }

    private void firePoolChangeEvent() {
        try {
            executorService.execute(new Runnable(){
                 @Override
                 public void run() {
                     try {
                         for (ClusteredBroker broker : brokers) {
                             broker.updatePoolState();
                         }
                     } catch (Exception e) {
                         FabricException.launderThrowable(e);
                     }
                 }
             });
        } catch (RejectedExecutionException e) {
            // Ignore these after the component shuts down.
            if( !executorService.isShutdown() ) {
                throw e;
            }
        }
    }

    public void add(ClusteredBroker broker) {
        brokers.add(broker);
    }

    public void remove(ClusteredBroker broker) {
        brokers.remove(broker);
    }

}
