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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.exceptions.ClassNotResolvedException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.flush.FlushOrdered;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.Extent;
import org.datanucleus.store.ObjectReferencingStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neodatis.odb.ClassRepresentation;
import org.neodatis.odb.ODB;
import org.neodatis.odb.OID;

/**
 * Store Manager for NeoDatis (http://www.neodatis.org/).
 * <p>
 * DataNucleus will select this StoreManager with URLs of the form "neodatis:...".
 * Support for NeoDatis is for the following URLs
 * <ul>
 * <li>neodatis:{filename} - uses a local NeoDatis database in the named file</li>
 * <li>neodatis:server:{filename} - uses a local NeoDatis database in the named file as an embedded server</li>
 * <li>neodatis:{hostname}:{port}/{identifier} - uses a remote NeoDatis database with the specified username/password</li>
 * <ul>
 * These URLs are specific to DataNucleus support since NeoDatis doesn't use URLs to define the datastore.
 * </p>
 * <h3>Object Activation and Field Loading</h3>
 * <p>
 * NeoDatis provides methods to hand out memory versions of the datastore objects. Each object has to be
 * "activated" to have its values accessible. Similarly the object can be "deactivated" when it is
 * no longer needed (freeing up resources, and the link to the disk object). When we retrieve an object
 * from NeoDatis it activates the object. This makes all fields accessible. If however one of the fields is
 * a PC object (or collection, or map, or array etc) that object itself is not activated. Consequently
 * all fields are accessible from the start (unlike with RDBMS), but the ObjectProvider manages the list
 * of fields that are considered "loaded", and this is initially just those in the DFG (even though others
 * are available). When the user accesses one of these "not-loaded" fields NeoDatisManager.fetchObject is called
 * and that field is marked as loaded (if it is a SCO mutable it is wrapped, and if it is a PC object it
 * has a ObjectProvider connected and is activated).
 * </p>
 * <h3>Persistence</h3>
 * <p>
 * Each object is persisted on its own, and we dont use NeoDatis's internal cascade mechanism. Instead
 * the NeoDatisManager.insertObject, or NeoDatisManager.updateObject methods are called for each object that
 * is to be persisted/updated. Each call to insertObject/updateObject provides reachability by use
 * of PersistFieldManager.
 * </p>
 * <h3>Deletion</h3>
 * <p>
 * Currently objects are deleted one by one since NeoDatis doesn't provide its own cascade delete process.
 * We use DeleteFieldManager to navigate through the objec graph according to dependent field metadata.
 * </p>
 * <h3>Transactions and Connections</h3>
 * <p>
 * Refer to ConnectionFactoryImpl. In simple terms each ExecutionContext has a NeoDatis ODB associated
 * with it, and this is retained until the end of life of the ExecutionContext. With NeoDatis in server mode this
 * allows use of multiple PMs on the same underlying datastore. With NeoDatis in file mode you can only
 * have one PM operating on the same underlying datastore at once. This is a NeoDatis limitation rather than ours.
 * </p>
 */
public class NeoDatisStoreManager extends AbstractStoreManager implements ObjectReferencingStoreManager
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_NEODATIS = Localiser.getInstance(
        "org.datanucleus.store.neodatis.Localisation", NeoDatisStoreManager.class.getClassLoader());

    /** 
     * Collection of the currently active ODBs.
     * Used for providing class mapping information when they are found.
     */
    private Set activeODBs = new HashSet();

    /**
     * Constructor for a new NeoDatis StoreManager. 
     * Stores the basic information required for the datastore management.
     * @param clr the ClassLoaderResolver
     * @param ctx The corresponding context.
     */
    public NeoDatisStoreManager(ClassLoaderResolver clr, NucleusContext ctx, Map<String, Object> props)
    {
        super("neodatis", clr, ctx, props);

        PersistenceConfiguration conf = ctx.getPersistenceConfiguration();
        if (!conf.getStringProperty("datanucleus.cache.level2.type").equalsIgnoreCase("none"))
        {
            // TODO Remove this when we can handle getting objects from the L2 cache
            NucleusLogger.PERSISTENCE.warn("NeoDatis StoreManager is not fully supported when using the L2 cache");
        }

        // Use unique datastore-identity ids (is this correct for NeoDatis???)
        conf.setProperty("datanucleus.datastoreIdentityType", "unique");

        // Log the manager configuration
        logConfiguration();

        // Handler for persistence process
        persistenceHandler = new NeoDatisPersistenceHandler(this);
        flushProcess = new FlushOrdered();

        // Make sure transactional connection factory has listener for closing object container
        ctx.addExecutionContextListener(new ExecutionContext.LifecycleListener()
        {
            public void preClose(ExecutionContext ec)
            {
                // Close any ODB for this ExecutionContext
                ConnectionFactoryImpl cf = 
                    (ConnectionFactoryImpl)connectionMgr.lookupConnectionFactory(primaryConnectionFactoryName);
                cf.closeODBForExecutionContext(ec);
            }
        });
    }

    /**
     * Release of resources
     */
    public void close()
    {
        super.close();
        activeODBs.clear();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getStrategyForNative(org.datanucleus.metadata.AbstractClassMetaData, int)
     */
    @Override
    protected String getStrategyForNative(AbstractClassMetaData cmd, int absFieldNumber)
    {
        return "increment";
    }

    // ------------------------------- Class Management -----------------------------------

    /**
     * Method to register some data with the store.
     * This will also register the data with the starter process.
     * @param data The StoreData to add
     */
    protected void registerStoreData(StoreData data)
    {
        // Register the data
        super.registerStoreData(data);

        // Make this class config known to all active ODB for this store
        if (activeODBs.size() > 0)
        {
            Iterator containerIter = activeODBs.iterator();
            while (containerIter.hasNext())
            {
                ODB odb = (ODB)containerIter.next();
                supportClassInNeoDatis(odb, (AbstractClassMetaData)data.getMetaData());
            }
        }
    }

    /**
     * Method to register an ODB as active on this store.
     * Will load up all known class mapping information into the datastore container.
     * @param odb ODB
     */
    public void registerODB(ODB odb)
    {
        if (odb == null)
        {
            return;
        }

        // Register all known classes with the ODB of this transaction
        activeODBs.add(odb);
    }

    /**
     * Method to deregister an ODB from this store.
     * ODB are deregistered when about to be closed and hence not interested in more class mapping information.
     * @param odb ODB
     */
    public void deregisterODB(ODB odb)
    {
        if (odb == null)
        {
            return;
        }
        activeODBs.remove(odb);
    }

    /**
     * Convenience method to support the specified class in NeoDatis.
     * Prepares the NeoDatis environment for the persistence of this class (indexed fields, persist-cascade etc).
     * TODO This will need update in the future to provide for schema updates, so where a class definition has
     * changed and we need to set the changes in NeoDatis.
     * @param odb The NeoDatis ODB
     * @param cmd MetaData for the class
     */
    private void supportClassInNeoDatis(ODB odb, AbstractClassMetaData cmd)
    {
        try
        {
            ClassRepresentation cr = odb.getClassRepresentation(cmd.getFullClassName());
            AbstractMemberMetaData[] fmds = cmd.getManagedMembers();
            for (int i=0;i<fmds.length;i++)
            {
                // Set the indexing for the field
                if (fmds[i].getUniqueMetaData() != null)
                {
                    // Add the unique index if not present. Provide default index name as "{fieldName}_IDX"
                    String indexName = fmds[i].getName().toUpperCase() + "_IDX";
                    if (fmds[i].getUniqueMetaData() != null && fmds[i].getUniqueMetaData().getName() != null)
                    {
                        indexName = fmds[i].getUniqueMetaData().getName();
                    }
                    if (!cr.existIndex(indexName))
                    {
                        cr.addUniqueIndexOn(indexName, new String[] {fmds[i].getName()}, false);
                    }
                }
                else if (fmds[i].isPrimaryKey() || fmds[i].getIndexMetaData() != null)
                {
                    // Add the index if not present. Provide default index name as "{fieldName}_IDX"
                    String indexName = fmds[i].getName().toUpperCase() + "_IDX";
                    if (fmds[i].getIndexMetaData() != null && fmds[i].getIndexMetaData().getName() != null)
                    {
                        indexName = fmds[i].getIndexMetaData().getName();
                    }
                    if (!cr.existIndex(indexName))
                    {
                        cr.addIndexOn(indexName, new String[] {fmds[i].getName()}, false);
                    }
                }
            }
        }
        catch (ClassNotResolvedException cnre)
        {
            // Do nothing
        }
    }

    /**
     * Hook for the ObjectProvider to notify us that an object is outdated (no longer valid).
     * With NeoDatis we have no activate/deactivate mechanism so we do nothing.
     * @param sm ObjectProvider of object
     */
    public void notifyObjectIsOutdated(ObjectProvider sm)
    {
    }

    // ------------------------------- Utilities -----------------------------------

    /**
     * Check if the strategy is attributed by the database when the PersistenceCapable object is inserted into 
     * the database. "datastore-identity" cases will be datastore attributed (since we can't store the 
     * id anywhere).
     * @param cmd Metadata for the class
     * @param absFieldNumber FieldNumber of the field (or -1 if datastore id)
     * @return if the object for the strategy is attributed by the database
     */
    public boolean isStrategyDatastoreAttributed(AbstractClassMetaData cmd, int absFieldNumber)
    {
        if (absFieldNumber < 0)
        {
            // datastore id
            return true;
        }
        else
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(absFieldNumber);
            if (mmd.getValueStrategy() == null)
            {
                return false;
            }
            else if (mmd.getValueStrategy() == IdentityStrategy.IDENTITY)
            {
                throw new NucleusException("datanucleus-neodatis doesnt currently support use of application-identity and strategy \"identity\"");
            }
            else if (mmd.getValueStrategy() == IdentityStrategy.NATIVE)
            {
                String strategy = getStrategyForNative(cmd, absFieldNumber);
                if (strategy.equalsIgnoreCase("identity"))
                {
                    throw new NucleusException("datanucleus-neodatis doesnt currently support use of application-identity and strategy \"identity\"");
                }
            }
        }

        return false;
    }

    /**
     * Convenience method to get the identity for a Persistable object.
     * @param ec execution context
     * @param pc The object
     * @return The identity
     */
    public Object getObjectIdForObject(ExecutionContext ec, Object pc)
    {
        AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(pc.getClass().getName(), 
            ec.getClassLoaderResolver());
        Object id = null;
        ObjectProvider sm = ec.findObjectProvider(pc);
        if (sm != null)
        {
            // Object is managed, so return its id
            return sm.getInternalObjectId();
        }

        ODB odb = (ODB)getConnection(ec).getConnection();
        try
        {
            // TODO If the object passed in is not activated in NeoDatis can we generate the id for app id cases ?
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Consult the ODB for the internal NeoDatis id for this object
                OID oid = odb.getObjectId(pc);
                long datastoreId = oid.getClassId();
                if (datastoreId == 0)
                {
                    // Not stored in the NeoDatis datastore
                    return null;
                }
                return OIDFactory.getInstance(getNucleusContext(), datastoreId);
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                // If the fields are loaded then the id is known
                return getApiAdapter().getNewApplicationIdentityObjectId(pc, cmd);
            }
        }
        finally
        {
        }

        return id;
    }

    /**
     * Accessor for an Extent for a class.
     * @param ec execution context
     * @param c The class requiring the Extent
     * @param subclasses Whether to include subclasses of 'c'
     * @return The Extent.
     */
    public Extent getExtent(ExecutionContext ec, Class c, boolean subclasses)
    {
        AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(c, ec.getClassLoaderResolver());
        if (!cmd.isRequiresExtent())
        {
            throw new NoExtentException(c.getName());
        }

        return new NeoDatisExtent(this, ec, c, subclasses, cmd);
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("DatastoreIdentity");
        set.add("ApplicationIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        return set;
    }
}