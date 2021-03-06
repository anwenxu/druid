/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.curator.inventory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.curator.ShutdownNowIgnoringExecutorService;
import io.druid.curator.cache.PathChildrenCacheFactory;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An InventoryManager watches updates to inventory on Zookeeper (or some other discovery-like service publishing
 * system).  It is built up on two object types: containers and inventory objects.
 * 
 * The logic of the InventoryManager just maintains a local cache of the containers and inventory it sees on ZK.  It
 * provides methods for getting at the container objects, which house the actual individual pieces of inventory.
 * 
 * A Strategy is provided to the constructor of an Inventory manager, this strategy provides all of the
 * object-specific logic to serialize, deserialize, compose and alter the container and inventory objects.
 */
public class CuratorInventoryManager<ContainerClass, InventoryClass>
{
  private static final Logger log = new Logger(CuratorInventoryManager.class);

  private final Object lock = new Object();

  private final CuratorFramework curatorFramework;
  private final InventoryManagerConfig config;
  private final CuratorInventoryManagerStrategy<ContainerClass, InventoryClass> strategy;

  private final ConcurrentMap<String, ContainerHolder> containers;
  private final Set<ContainerHolder> uninitializedInventory;
  private final PathChildrenCacheFactory cacheFactory;

  private volatile PathChildrenCache childrenCache;

  public CuratorInventoryManager(
      CuratorFramework curatorFramework,
      InventoryManagerConfig config,
      ExecutorService exec,
      CuratorInventoryManagerStrategy<ContainerClass, InventoryClass> strategy
  )
  {
    this.curatorFramework = curatorFramework;
    this.config = config;
    this.strategy = strategy;

    this.containers = new MapMaker().makeMap();
    this.uninitializedInventory = Sets.newConcurrentHashSet();

    this.cacheFactory = new SimplePathChildrenCacheFactory(true, true, new ShutdownNowIgnoringExecutorService(exec));
  }

  @LifecycleStart
  public void start() throws Exception
  {
    synchronized (lock) {
      if (childrenCache != null) {
        return;
      }

      childrenCache = cacheFactory.make(curatorFramework, config.getContainerPath());
    }

    childrenCache.getListenable().addListener(new ContainerCacheListener());

    try {
      childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }
    catch (Exception e) {
      synchronized (lock) {
        try {
          stop();
        }
        catch (IOException e1) {
          log.error(e1, "Exception when stopping InventoryManager that couldn't start.");
        }
      }
      throw e;
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    synchronized (lock) {
      if (childrenCache == null) {
        return;
      }

      // This close() call actually calls shutdownNow() on the executor registered with the Cache object...
      childrenCache.close();
      childrenCache = null;
    }

    for (String containerKey : Lists.newArrayList(containers.keySet())) {
      final ContainerHolder containerHolder = containers.remove(containerKey);
      if (containerHolder == null) {
        log.wtf("!?  Got key[%s] from keySet() but it didn't have a value!?", containerKey);
      } else {
        // This close() call actually calls shutdownNow() on the executor registered with the Cache object...
        containerHolder.getCache().close();
      }
    }
  }

  public InventoryManagerConfig getConfig()
  {
    return config;
  }

  public ContainerClass getInventoryValue(String containerKey)
  {
    final ContainerHolder containerHolder = containers.get(containerKey);
    return containerHolder == null ? null : containerHolder.getContainer();
  }

  public Iterable<ContainerClass> getInventory()
  {
    return Iterables.transform(
        containers.values(),
        new Function<ContainerHolder, ContainerClass>()
        {
          @Override
          public ContainerClass apply(ContainerHolder input)
          {
            return input.getContainer();
          }
        }
    );
  }

  private class ContainerHolder
  {
    private final AtomicReference<ContainerClass> container;
    private final PathChildrenCache cache;
    private boolean initialized = false;

    ContainerHolder(
        ContainerClass container,
        PathChildrenCache cache
    )
    {
      this.container = new AtomicReference<ContainerClass>(container);
      this.cache = cache;
    }

    private ContainerClass getContainer()
    {
      return container.get();
    }

    private void setContainer(ContainerClass newContainer)
    {
      container.set(newContainer);
    }

    private PathChildrenCache getCache()
    {
      return cache;
    }
  }

  private class ContainerCacheListener implements PathChildrenCacheListener
  {
    private volatile boolean containersInitialized = false;
    private volatile boolean doneInitializing = false;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
      switch (event.getType()) {
        case CHILD_ADDED:
          synchronized (lock) {
            final ChildData child = event.getData();
            final String containerKey = ZKPaths.getNodeFromPath(child.getPath());

            final ContainerClass container = strategy.deserializeContainer(child.getData());

            // This would normally be a race condition, but the only thing that should be mutating the containers
            // map is this listener, which should never run concurrently.  If the same container is going to disappear
            // and come back, we expect a removed event in between.
            if (containers.containsKey(containerKey)) {
              log.error("New node[%s] but there was already one.  That's not good, ignoring new one.", child.getPath());
            } else {
              final String inventoryPath = String.format("%s/%s", config.getInventoryPath(), containerKey);
              PathChildrenCache inventoryCache = cacheFactory.make(curatorFramework, inventoryPath);
              inventoryCache.getListenable().addListener(new InventoryCacheListener(containerKey, inventoryPath));

              containers.put(containerKey, new ContainerHolder(container, inventoryCache));

              log.info("Starting inventory cache for %s, inventoryPath %s", containerKey, inventoryPath);
              inventoryCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
              strategy.newContainer(container);
            }

            break;
          }
        case CHILD_REMOVED:
          synchronized (lock) {
            final ChildData child = event.getData();
            final String containerKey = ZKPaths.getNodeFromPath(child.getPath());

            final ContainerHolder removed = containers.remove(containerKey);
            if (removed == null) {
              log.warn("Container[%s] removed that wasn't a container!?", child.getPath());
              break;
            }

            // This close() call actually calls shutdownNow() on the executor registered with the Cache object, it
            // better have its own executor or ignore shutdownNow() calls...
            log.info("Closing inventory cache for %s. Also removing listeners.", containerKey);
            removed.getCache().getListenable().clear();
            removed.getCache().close();
            strategy.deadContainer(removed.getContainer());

            // also remove node from uninitilized, in case a nodes gets removed while we are starting up
            synchronized (removed) {
              markInventoryInitialized(removed);
            }

            break;
          }
        case CHILD_UPDATED:
          synchronized (lock) {
            final ChildData child = event.getData();
            final String containerKey = ZKPaths.getNodeFromPath(child.getPath());

            final ContainerClass container = strategy.deserializeContainer(child.getData());

            ContainerHolder oldContainer = containers.get(containerKey);
            if (oldContainer == null) {
              log.warn("Container update[%s], but the old container didn't exist!?  Ignoring.", child.getPath());
            } else {
              synchronized (oldContainer) {
                oldContainer.setContainer(strategy.updateContainer(oldContainer.getContainer(), container));
              }
            }

            break;
          }
        case INITIALIZED:
          synchronized (lock) {
            // must await initialized of all containerholders
            for(ContainerHolder holder : containers.values()) {
              synchronized (holder) {
                if(!holder.initialized) {
                  uninitializedInventory.add(holder);
                }
              }
            }
            containersInitialized = true;
            maybeDoneInitializing();
            break;
          }
      }
    }

    // must be run in synchronized(lock) { synchronized(holder) { ... } } block
    private void markInventoryInitialized(final ContainerHolder holder)
    {
      holder.initialized = true;
      uninitializedInventory.remove(holder);
      maybeDoneInitializing();
    }

    private void maybeDoneInitializing()
    {
      if(doneInitializing) {
        return;
      }

      // only fire if we are done initializing the parent PathChildrenCache
      if(containersInitialized && uninitializedInventory.isEmpty()) {
        doneInitializing = true;
        strategy.inventoryInitialized();
      }
    }

    private class InventoryCacheListener implements PathChildrenCacheListener
    {
      private final String containerKey;
      private final String inventoryPath;

      public InventoryCacheListener(String containerKey, String inventoryPath)
      {
        this.containerKey = containerKey;
        this.inventoryPath = inventoryPath;

        log.info("Created new InventoryCacheListener for %s", inventoryPath);
      }

      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
      {
        final ContainerHolder holder = containers.get(containerKey);
        if (holder == null) {
          return;
        }

        switch (event.getType()) {
          case CHILD_ADDED: {
            final ChildData child = event.getData();
            final String inventoryKey = ZKPaths.getNodeFromPath(child.getPath());

            final InventoryClass addedInventory = strategy.deserializeInventory(child.getData());

            synchronized (holder) {
              holder.setContainer(strategy.addInventory(holder.getContainer(), inventoryKey, addedInventory));
            }
            break;
          }

          case CHILD_UPDATED: {
            final ChildData child = event.getData();
            final String inventoryKey = ZKPaths.getNodeFromPath(child.getPath());

            final InventoryClass updatedInventory = strategy.deserializeInventory(child.getData());

            synchronized (holder) {
              holder.setContainer(strategy.updateInventory(holder.getContainer(), inventoryKey, updatedInventory));
            }

            break;
          }

          case CHILD_REMOVED: {
            final ChildData child = event.getData();
            final String inventoryKey = ZKPaths.getNodeFromPath(child.getPath());

            synchronized (holder) {
              holder.setContainer(strategy.removeInventory(holder.getContainer(), inventoryKey));
            }

            break;
          }
          case INITIALIZED:
            // make sure to acquire locks in (lock -> holder) order
            synchronized (lock) {
              synchronized (holder) {
                markInventoryInitialized(holder);
              }
            }

            break;
        }
      }
    }
  }
}
