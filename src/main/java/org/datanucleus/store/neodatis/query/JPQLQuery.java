/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
 **********************************************************************/
package org.datanucleus.store.neodatis.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.inmemory.JPQLInMemoryEvaluator;
import org.datanucleus.query.inmemory.JavaQueryInMemoryEvaluator;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neodatis.NeoDatisUtils;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neodatis.odb.ODB;
import org.neodatis.odb.impl.core.query.criteria.CriteriaQuery;

/**
 * NeoDatis representation of a JPQL query for use by DataNucleus.
 * The query can be specified via method calls, or via a single-string form.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    private static final long serialVersionUID = -5582158225342925005L;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery)null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    protected Object performExecute(Map parameters)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        if (candidateCollection != null && candidateCollection.isEmpty())
        {
            return Collections.EMPTY_LIST;
        }

        boolean inMemory = evaluateInMemory();
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        try
        {
            ODB odb = (ODB) mconn.getConnection();

            // Execute the query
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "JPQL", getSingleStringQuery(), null));
            }
            List candidates = null;
            boolean filterInMemory = false;
            boolean orderingInMemory = false;
            if (candidateCollection == null)
            {
                // Create the Criteria query, optionally with the candidate and filter restrictions
                CriteriaQuery query = createCriteriaQuery(odb, compilation, parameters, inMemory);
                candidates = new ArrayList(odb.getObjects(query));
                if (inMemory)
                {
                    filterInMemory = true;
                    orderingInMemory = true;
                }
            }
            else
            {
                candidates = new ArrayList(candidateCollection);
                filterInMemory = true;
                orderingInMemory = true;
            }

            // Apply any restrictions to the results (that we can't use in the input Criteria query)
            JavaQueryInMemoryEvaluator resultMapper =
                new JPQLInMemoryEvaluator(this, candidates, compilation, parameters, clr);
            Collection results = resultMapper.execute(filterInMemory, orderingInMemory, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", "JPQL", "" + (System.currentTimeMillis() - startTime)));
            }

            Iterator iter = results.iterator();
            while (iter.hasNext())
            {
                Object obj = iter.next();
                if (result == null)
                {
                    // Assign ObjectProviders to any returned objects
                    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(obj.getClass(), clr);
                    NeoDatisUtils.prepareNeoDatisObjectForUse(obj, ec, odb, cmd);
                }
            }

            if (type == QueryType.BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == QueryType.BULK_UPDATE)
            {
                throw new NucleusException("Bulk Update is not yet supported");
            }
            else
            {
                return results;
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to create the Criteria query object for the candidate class, and with the possible
     * restrictions we can apply to the filter.
     * @param cont ObjectContainer
     * @param compilation The compilation results
     * @param parameters Any parameters
     * @param inMemory whether to process everything in-memory
     * @return The NeoDatis Criteria Query
     */
    private CriteriaQuery createCriteriaQuery(ODB odb, QueryCompilation compilation, Map parameters,
            boolean inMemory)
    {
        CriteriaQuery query = new CriteriaQuery(candidateClass);
        if (NucleusLogger.QUERY.isDebugEnabled())
        {
            NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria",
                "CriteriaQuery query = new CriteriaQuery(" + candidateClass.getName() + ")"));
        }
        if (subclasses)
        {
            query.setPolymorphic(true);
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("NeoDatis.Criteria",
                    "query.setPolymorphic(true)"));
            }
        }

        if (!inMemory)
        {
            // Constrain the query with filter and ordering constraints
            new QueryToCriteriaMapper(query, compilation, parameters).compile();
        }
        return query;
    }
}