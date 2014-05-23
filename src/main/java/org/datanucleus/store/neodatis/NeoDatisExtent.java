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
package org.datanucleus.store.neodatis;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractExtent;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neodatis.odb.ODB;
import org.neodatis.odb.Objects;
import org.neodatis.odb.core.query.IQuery;
import org.neodatis.odb.impl.core.query.criteria.CriteriaQuery;

/**
 * Extent for use with NeoDatis datastores.
 * The Extent is generated in two ways, with or without subclasses. These simply execute
 * a NeoDatis Criteria query setting the "polymorphic" flag.
 */
public class NeoDatisExtent extends AbstractExtent
{
    private NeoDatisStoreManager storeMgr;

    /** Set of iterators created by this Extent. */
    private Set iterators = new HashSet();

    /** FetchPlan for use with this Extent. */
    private FetchPlan fetchPlan = null;

    /**
     * Constructor.
     * @param storeMgr StoreManager
     * @param ec execution context
     * @param cls candidate class
     * @param subclasses Whether to include subclasses
     * @param cmd MetaData for the candidate class
     */
    public NeoDatisExtent(NeoDatisStoreManager storeMgr, ExecutionContext ec, Class cls, boolean subclasses, AbstractClassMetaData cmd)
    {
        super(ec, cls, subclasses, cmd);
        this.storeMgr = storeMgr;
        this.fetchPlan = ec.getFetchPlan().getCopy();
    }

    /**
     * Method to close the extent.
     * @param iter an iterator obtained by the method iterator() on this Extent instance.
     */
    public void close(Iterator iter)
    {
        iterators.remove(iter);
    }

    /**
     * Close all Iterators associated with this Extent instance. Iterators closed by this method will return false to
     * hasNext() and will throw NoSuchElementException on next(). The Extent instance can still be used as a parameter
     * of Query.setCandidates, and to get an Iterator.
     */
    public void closeAll()
    {
        iterators.clear();
    }

    /**
     * Returns an iterator over all the instances in the Extent.
     * @return an iterator over all the instances in the Extent.
     */
    public Iterator iterator()
    {
        // Retrieve the possible instances
        ManagedConnection mconn = storeMgr.getConnection(ec);
        ODB odb = (ODB) mconn.getConnection();
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("NeoDatis.Extent.Execute", candidateClass, "" + subclasses));
            }
            Objects results = null;
            IQuery query = new CriteriaQuery(candidateClass);
            query.setPolymorphic(subclasses);
            try
            {
                results = odb.getObjects(query);
            }
            catch (Exception e)
            {
                throw new NucleusException("Exception thrown executing query", e);
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("NeoDatis.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
    
            NeoDatisExtentIterator iter = new NeoDatisExtentIterator(results);
            iterators.add(iter);
            return iter;
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * This method retrieves the fetch plan associated with the Extent. It always returns the identical instance for the
     * same Extent instance. Any change made to the fetch plan affects subsequent instance retrievals via next(). Only
     * instances not already in memory are affected by the fetch plan. Fetch plan is described in Section 12.7.
     * @return the FetchPlan
     */
    public FetchPlan getFetchPlan()
    {
        return fetchPlan;
    }

    /**
     * Iterator for use with NeoDatis Extents.
     */
    public class NeoDatisExtentIterator implements Iterator
    {
        /** The NeoDatis ObjectSet for the Extent. */
        Objects objects = null;

        /**
         * Constructor.
         * @param objects The NeoDatis query results
         */
        public NeoDatisExtentIterator(Objects objects)
        {
            this.objects = objects;
        }

        /**
         * Method to return if there is another object in the iterator.
         * @return Whether there is another object
         */
        public boolean hasNext()
        {
            return objects.hasNext();
        }

        /**
         * Method to return the next object in the iterator.
         * @return The next object
         */
        public Object next()
        {
            Object obj = objects.next();

            ManagedConnection mconn = storeMgr.getConnection(ec);
            ODB odb = (ODB) mconn.getConnection();
            try
            {
                NeoDatisUtils.prepareNeoDatisObjectForUse(obj, ec, odb, cmd);
            }
            finally
            {
                mconn.release();
            }

            return obj;
        }

        /**
         * Method to remove an object.
         */
        public void remove()
        {
            throw new UnsupportedOperationException(Localiser.msg("NeoDatis.Extent.IteratorRemoveNotSupported"));
        }
    }
}