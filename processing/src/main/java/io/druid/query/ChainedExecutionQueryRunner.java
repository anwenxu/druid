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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.MergeIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A QueryRunner that combines a list of other QueryRunners and executes them in parallel on an executor.
 *
 * When using this, it is important to make sure that the list of QueryRunners provided is fully flattened.
 * If, for example, you were to pass a list of a Chained QueryRunner (A) and a non-chained QueryRunner (B).  Imagine
 * A has 2 QueryRunner chained together (Aa and Ab), the fact that the Queryables are run in parallel on an
 * executor would mean that the Queryables are actually processed in the order
 *
 * <pre>A -&gt; B -&gt; Aa -&gt; Ab</pre>
 *
 * That is, the two sub queryables for A would run *after* B is run, effectively meaning that the results for B
 * must be fully cached in memory before the results for Aa and Ab are computed.
 */
public class ChainedExecutionQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ChainedExecutionQueryRunner.class);

  private final Iterable<QueryRunner<T>> queryables;
  private final ListeningExecutorService exec;
  private final Ordering<T> ordering;
  private final QueryWatcher queryWatcher;

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      Ordering<T> ordering,
      QueryWatcher queryWatcher,
      QueryRunner<T>... queryables
  )
  {
    this(exec, ordering, queryWatcher, Arrays.asList(queryables));
  }

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      Ordering<T> ordering,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<T>> queryables
  )
  {
    // listeningDecorator will leave PrioritizedExecutorService unchanged,
    // since it already implements ListeningExecutorService
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.ordering = ordering;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final int priority = query.getContextPriority(0);

    return new BaseSequence<T, Iterator<T>>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            // Make it a List<> to materialize all of the values (so that it will submit everything to the executor)
            ListenableFuture<List<Iterable<T>>> futures = Futures.allAsList(
                Lists.newArrayList(
                    Iterables.transform(
                        queryables,
                        new Function<QueryRunner<T>, ListenableFuture<Iterable<T>>>()
                        {
                          @Override
                          public ListenableFuture<Iterable<T>> apply(final QueryRunner<T> input)
                          {
                            return exec.submit(
                                new AbstractPrioritizedCallable<Iterable<T>>(priority)
                                {
                                  @Override
                                  public Iterable<T> call() throws Exception
                                  {
                                    try {
                                      if (input == null) {
                                        throw new ISE("Input is null?! How is this possible?!");
                                      }

                                      Sequence<T> result = input.run(query);
                                      if (result == null) {
                                        throw new ISE("Got a null result! Segments are missing!");
                                      }

                                      List<T> retVal = Sequences.toList(result, Lists.<T>newArrayList());
                                      if (retVal == null) {
                                        throw new ISE("Got a null list of results! WTF?!");
                                      }

                                      return retVal;
                                    }
                                    catch (QueryInterruptedException e) {
                                      throw Throwables.propagate(e);
                                    }
                                    catch (Exception e) {
                                      log.error(e, "Exception with one of the sequences!");
                                      throw Throwables.propagate(e);
                                    }
                                  }
                                }
                            );
                          }
                        }
                    )
                )
            );

            queryWatcher.registerQuery(query, futures);

            try {
              final Number timeout = query.getContextValue("timeout", (Number)null);
              return new MergeIterable<>(
                  ordering.nullsFirst(),
                  timeout == null ?
                  futures.get() :
                  futures.get(timeout.longValue(), TimeUnit.MILLISECONDS)
              ).iterator();
            }
            catch (InterruptedException e) {
              log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
              futures.cancel(true);
              throw new QueryInterruptedException("Query interrupted");
            }
            catch(CancellationException e) {
              throw new QueryInterruptedException("Query cancelled");
            }
            catch(TimeoutException e) {
              log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
              futures.cancel(true);
              throw new QueryInterruptedException("Query timeout");
            }
            catch (ExecutionException e) {
              throw Throwables.propagate(e.getCause());
            }
          }

          @Override
          public void cleanup(Iterator<T> tIterator)
          {

          }
        }
    );
  }
}
