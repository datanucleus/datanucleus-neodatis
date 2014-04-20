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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.DatastoreUniqueOID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.fieldmanager.PersistFieldManager;
import org.datanucleus.store.neodatis.fieldmanager.RetrieveFieldManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.neodatis.odb.ODB;
import org.neodatis.odb.ODBRuntimeException;
import org.neodatis.odb.OID;
import org.neodatis.odb.Objects;
import org.neodatis.odb.core.query.criteria.And;
import org.neodatis.odb.core.query.criteria.ICriterion;
import org.neodatis.odb.core.query.criteria.Where;
import org.neodatis.odb.impl.core.oid.OdbObjectOID;
import org.neodatis.odb.impl.core.query.criteria.CriteriaQuery;

/**
 * Persistence handler for persisting to NeoDatis datastores.
 */
public class NeoDatisPersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_NEODATIS = Localiser.getInstance(
        "org.datanucleus.store.neodatis.Localisation", NeoDatisStoreManager.class.getClassLoader());

    // Temporary debug flag that allows easy generation of the calls going into NeoDatis
    public static boolean neodatisDebug = false;

    /**
     * Thread-specific state information (instances of {@link OperationInfo}) for inserting.
     * Allows us to detect the primary object to be inserted, so we can call NeoDatis with that
     * and not for any others.
     */
    private ThreadLocal insertInfoThreadLocal = new ThreadLocal()
    {
        protected Object initialValue()
        {
            return new OperationInfo();
        }
    };

    private static class OperationInfo
    {
        /** List of objects to perform the operation on. */
        List objectsList = null;
    }

    /**
     * Thread-specific state information (instances of {@link OperationInfo}) for deleting.
     * Allows us to detect the primary object to be deleted, so we can call NeoDatis with that
     * and not for any others.
     */
    /*private ThreadLocal deleteInfoThreadLocal = new ThreadLocal()
    {
        protected Object initialValue()
        {
            return new OperationInfo();
        }
    };*/

    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public NeoDatisPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /**
     * Method to close the handler and release any resources.
     */
    public void close()
    {
        // No resources to clean up
    }

    /**
     * Inserts a persistent object into the database.
     * Provides persist-by-reachability using PersistFieldManager to go through all related PC fields.
     * If this is the principal object for this thread (the object that the user invoked makePersistent on)
     * then this will actually update the object in NeoDatis when all reachables have been processed. 
     * The reachability process means that we assign ObjectProviders down the object graph, and that object 
     * states are set up. Only a single NeoDatis "set" command is invoked per user makePersistent() call, 
     * using NeoDatis' ability to persist by reachability.
     * @param sm The state manager of the object to be inserted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void insertObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        if (sm.getClassMetaData().getIdentityType() == IdentityType.APPLICATION)
        {
            // Check existence of the object since NeoDatis doesn't enforce application identity
            try
            {
                locateObject(sm);
                throw new NucleusUserException(LOCALISER_NEODATIS.msg("NeoDatis.Insert.ObjectWithIdAlreadyExists",
                    sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }
            catch (NucleusObjectNotFoundException onfe)
            {
                // Do nothing since object with this id doesn't exist
            }
        }

        String className = sm.getObject().getClass().getName();
        if (!storeMgr.managesClass(className))
        {
            // Class is not yet registered here so register it
            storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), className);
        }

        // Get the InsertInfo for this thread so we know if this is the primary object or a reachable
        OperationInfo insertInfo = (OperationInfo) insertInfoThreadLocal.get();
        boolean primaryObject = false;
        if (insertInfo.objectsList == null)
        {
            // Primary object
            primaryObject = true;
            insertInfo.objectsList = new ArrayList();
        }
        insertInfo.objectsList.add(sm.getObject());

        // Perform reachability, wrapping all reachable PCs with ObjectProviders (recurses through here)
        // Doesn't replace SCOs with wrappers at this point since we don't want them in NeoDatis
        sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new PersistFieldManager(sm, false));

        if (primaryObject)
        {
            // Primary object (after reachability processing) so persist all objects
            ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
            try
            {
                ODB odb = (ODB)mconn.getConnection();

                // Insert the object(s)
                long startTime = 0;
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    startTime = System.currentTimeMillis();
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Insert.Start", 
                        sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                }

                // Persist all reachable objects
                if (neodatisDebug)
                {
                    System.out.println(">> insertObject odb.store(" + sm.getObjectAsPrintable() + ")");
                }
                odb.store(sm.getObject());

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }

                // Process all reachable objects since all are now persistent
                Iterator objsIter = insertInfo.objectsList.iterator();
                while (objsIter.hasNext())
                {
                    Object pc = objsIter.next();
                    processInsertedObject(odb, sm.getExecutionContext(), pc);
                }
            }
            catch (ODBRuntimeException re)
            {
                throw new NucleusDataStoreException("Exception throw inserting " +
                    sm.getObjectAsPrintable() + " (and reachables) in datastore", re);
            }
            finally
            {
                mconn.release();
            }

            // Clean out the OperationInfo for inserts on this thread
            insertInfo.objectsList.clear();
            insertInfo.objectsList = null;
            insertInfoThreadLocal.remove();
        }
    }

    /**
     * Method to take an object that has just been persisted to NeoDatis and furnish its ObjectProvider
     * with any important information, such as version, object id, etc.
     * @param odb ODB
     * @param ec execution context
     * @param pc The persistable object
     */
    private void processInsertedObject(ODB odb, ExecutionContext ec, Object pc)
    {
        if (ec.getStatistics() != null)
        {
            ec.getStatistics().incrementNumWrites();
            ec.getStatistics().incrementInsertCount();
        }

        ObjectProvider objSM = ec.findObjectProvider(pc);
        if (objSM != null)
        {
            // Update the object with any info populated by NeoDatis
            AbstractClassMetaData cmd = objSM.getClassMetaData();
            OID oid = null;
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // datastore-identity is always attributed by NeoDatis, using internal NeoDatis identity
                if (neodatisDebug)
                {
                    System.out.println(">> insertObject odb.getObjectId(" + StringUtils.toJVMIDString(pc) + ")");
                }
                oid = odb.getObjectId(pc);
                long datastoreId = oid.getObjectId();
                if (datastoreId > 0)
                {
                    objSM.setPostStoreNewObjectId(OIDFactory.getInstance(ec.getNucleusContext(), datastoreId));
                }
                else
                {
                    String msg = LOCALISER_NEODATIS.msg("NeoDatis.Insert.ObjectPersistFailed",
                        StringUtils.toJVMIDString(pc));
                    NucleusLogger.DATASTORE.error(msg);
                    throw new NucleusDataStoreException(msg);
                }
            }

            if (cmd.isVersioned())
            {
                // versioned object so update its version to what was just stored
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    int versionNumber = odb.ext().getObjectVersion(oid, true);
                    objSM.setTransactionalVersion(Long.valueOf(versionNumber));
                    NucleusLogger.DATASTORE.debug(LOCALISER_NEODATIS.msg("NeoDatis.Insert.ObjectPersistedWithVersion",
                        StringUtils.toJVMIDString(pc), objSM.getInternalObjectId(), "" + versionNumber));
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    long updateTimestamp = odb.ext().getObjectUpdateDate(oid, true);
                    Timestamp ts = new Timestamp(updateTimestamp);
                    objSM.setTransactionalVersion(ts);
                    NucleusLogger.DATASTORE.debug(LOCALISER_NEODATIS.msg("NeoDatis.Insert.ObjectPersistedWithVersion",
                        StringUtils.toJVMIDString(pc), objSM.getInternalObjectId(), "" + ts));
                }
            }
            else
            {
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Insert.ObjectPersisted",
                        StringUtils.toJVMIDString(pc), objSM.getInternalObjectId()));
                }
            }

            // Wrap any unwrapped SCO fields so we can pick up subsequent changes
            objSM.replaceAllLoadedSCOFieldsWithWrappers();
        }
    }

    /**
     * Updates a persistent object in the database.
     * Provides reachability via PersistFieldManager, meaning that all PC fields that have been updated will be
     * attached/updated too. Each call to "update" here will result in a call to NeoDatis "set".
     * @param sm The state manager of the object to be updated.
     * @param fieldNumbers The numbers of the fields to be updated.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails
     */
    public void updateObject(ObjectProvider sm, int fieldNumbers[])
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        // Perform reachability, wrapping all reachable PCs with ObjectProviders (recurses through this method)
        sm.provideFields(fieldNumbers, new PersistFieldManager(sm, false));

        // Unwrap any SCO wrapped fields so we don't persist them
        sm.replaceAllLoadedSCOFieldsWithValues();

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        AbstractClassMetaData cmd = sm.getClassMetaData();
        try
        {
            ODB odb = (ODB)mconn.getConnection();
            if (ec.getTransaction().getOptimistic() && cmd.isVersioned())
            {
                // Optimistic transaction so perform version check before any update
                OID oid = null;
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    long idNumber = ((DatastoreUniqueOID)sm.getInternalObjectId()).getKey();
                    oid = new OdbObjectOID(idNumber);
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    oid = odb.getObjectId(sm.getObject());
                }

                if (oid != null)
                {
                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                    {
                        long datastoreVersion = odb.ext().getObjectVersion(oid, true);
                        if (datastoreVersion > 0)
                        {
                            VersionHelper.performVersionCheck(sm, Long.valueOf(datastoreVersion), vermd);
                        }
                    }
                    else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                    {
                        long ts = odb.ext().getObjectUpdateDate(oid, true);
                        if (ts > 0)
                        {
                            VersionHelper.performVersionCheck(sm, new Timestamp(ts), vermd);
                        }
                    }
                }
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuffer fieldStr = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Update.Start", 
                    sm.getObjectAsPrintable(), sm.getInternalObjectId(), fieldStr.toString()));
            }

            if (!sm.getAllFieldsLoaded())
            {
                // Some fields not loaded, so maybe attaching. Load our object from the datastore first
                Object obj = sm.getObject();
                int[] dirtyFieldNumbers = sm.getDirtyFieldNumbers();
                if (dirtyFieldNumbers != null && dirtyFieldNumbers.length > 0)
                {
                    // Some fields need preserving
                    Object copy = storeMgr.getApiAdapter().getCopyOfPersistableObject(obj, sm, dirtyFieldNumbers);
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Object.Refreshing", 
                            StringUtils.toJVMIDString(obj)));
                    }
                    // TODO Retrieve the current object
                    storeMgr.getApiAdapter().copyFieldsFromPersistableObject(copy, dirtyFieldNumbers, obj);
                }
                else
                {
                    // Nothing dirty
                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Object.Refreshing", 
                            StringUtils.toJVMIDString(obj)));
                    }
                    // TODO Replace with a new retrieved object
                }
            }

            // Update the object in the datastore
            try
            {
                if (neodatisDebug)
                {
                    System.out.println(">> updateObject odb.getObjectId(" + sm.getObjectAsPrintable() + ")");
                }
                odb.getObjectId(sm.getObject());
            }
            catch (ODBRuntimeException re)
            {
                // This object is not now managed so need to retrieve it
                throw new NucleusDataStoreException("Object not found when preparing for update " + 
                    sm.getObjectAsPrintable(), re);
            }

            if (neodatisDebug)
            {
                System.out.println(">> updateObject odb.store(" + sm.getObjectAsPrintable() + ")");
            }
            odb.store(sm.getObject());
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (cmd.isVersioned())
            {
                // Optimistic transaction so update the (transactional) version in the object
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                OID oid = odb.getObjectId(sm.getObject());
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long datastoreVersion = odb.ext().getObjectVersion(oid, true);
                    if (datastoreVersion > 0)
                    {
                        sm.setTransactionalVersion(Long.valueOf(datastoreVersion));
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    long ts = odb.ext().getObjectUpdateDate(oid, true);
                    if (ts > 0)
                    {
                        sm.setTransactionalVersion(new Timestamp(ts));
                    }
                }
            }

            // Wrap any unwrapped SCO fields so any subsequent changes are picked up
            sm.replaceAllLoadedSCOFieldsWithWrappers();
        }
        catch (ODBRuntimeException re)
        {
            throw new NucleusDataStoreException("Exception thrown updating " +
                sm.getObjectAsPrintable() + " in datastore", re);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes a persistent object from the database.
     * Performs delete-by-reachability ("cascade-delete") via DeleteFieldManager. If this is the primary 
     * object to be deleted (via the users deletePersistent call) then after processing the object graph 
     * will call NeoDatis "delete" for that object and use NeoDatis' delete by reachability.
     * @param sm The state manager of the object to be deleted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction for this object
     */
    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            ODB odb = (ODB)mconn.getConnection();
            if (ec.getTransaction().getOptimistic() && cmd.isVersioned())
            {
                // Optimistic transaction so perform version check before any delete
                OID oid = null;
                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    long idNumber = ((DatastoreUniqueOID)sm.getInternalObjectId()).getKey();
                    oid = new OdbObjectOID(idNumber);
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    oid = odb.getObjectId(sm.getObject());
                }

                if (oid != null)
                {
                    VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                    if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                    {
                        long datastoreVersion = odb.ext().getObjectVersion(oid, true);
                        if (datastoreVersion > 0)
                        {
                            VersionHelper.performVersionCheck(sm, Long.valueOf(datastoreVersion), vermd);
                        }
                    }
                    else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                    {
                        long ts = odb.ext().getObjectUpdateDate(oid, true);
                        if (ts > 0)
                        {
                            VersionHelper.performVersionCheck(sm, new Timestamp(ts), vermd);
                        }
                    }
                }
            }

            // Load any unloaded fields so that DeleteFieldManager has all field values to work with
            sm.loadUnloadedFields();

            try
            {
                // Delete all reachable PC objects (due to dependent-field). Updates lifecycle to P_DELETED
                sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new DeleteFieldManager(sm));

                long startTime = System.currentTimeMillis();
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Delete.Start", 
                        sm.getObjectAsPrintable(), sm.getInternalObjectId()));
                }

                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    long idNumber = ((DatastoreUniqueOID)sm.getInternalObjectId()).getKey();
                    OID oid = new OdbObjectOID(idNumber);
                    if (neodatisDebug)
                    {
                        System.out.println(">> deleteObject odb.deleteObjectWithId(" + oid + ")");
                    }
                    odb.deleteObjectWithId(oid);
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    if (neodatisDebug)
                    {
                        System.out.println(">> deleteObject odb.delete(" + sm.getObjectAsPrintable() + ")");
                    }
                    odb.delete(sm.getObject());
                }

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }
                if (ec.getStatistics() != null)
                {
                    ec.getStatistics().incrementNumWrites();
                    ec.getStatistics().incrementDeleteCount();
                }
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    // Debug info about the deleted object
                    ObjectProvider objSM = sm.getExecutionContext().findObjectProvider(sm.getObject());
                    if (objSM != null)
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_NEODATIS.msg("NeoDatis.Delete.ObjectDeleted", 
                            sm.getObjectAsPrintable(), objSM.getInternalObjectId()));
                    }
                }
            }
            catch (ODBRuntimeException re)
            {
                throw new NucleusDataStoreException("Exception thrown deleting " +
                    sm.getObjectAsPrintable() + " from datastore", re);
            }
        }
        finally
        {
            mconn.release();
        }
    }

    // NOTE : This is how deleteObject should be like when NeoDatis supports cascade-delete(not in 1.9)
    /**
     * Deletes a persistent object from the database.
     * Performs delete-by-reachability ("cascade-delete") via DeleteFieldManager. If this is the primary 
     * object to be deleted (via the users deletePersistent call) then after processing the object graph 
     * will call NeoDatis "delete" for that object and use NeoDatis' delete by reachability.
     * @param sm The state manager of the object to be deleted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction for this object
     */
    /*public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        // Get the OperationInfo for this thread so we know if this is the primary object or a reachable
        OperationInfo deleteInfo = (OperationInfo) deleteInfoThreadLocal.get();
        boolean primaryObject = false;
        if (deleteInfo.objectsList == null)
        {
            // Primary object
            primaryObject = true;
            deleteInfo.objectsList = new ArrayList();
        }
        deleteInfo.objectsList.add(sm.getObject());

        AbstractClassMetaData cmd = sm.getClassMetaData();
        VersionMetaData vermd = cmd.getVersionMetaData();
        ODB odb = (ODB)storeMgr.getConnection(sm.getExecutionContext()).getConnection();
        if (sm.getExecutionContext().getTransaction().getOptimistic() && vermd != null)
        {
            // Optimistic transaction so perform version check before any delete
            OID oid = null;
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                long idNumber = ((DatastoreUniqueOID)sm.getInternalObjectId()).getKey();
                oid = new OdbObjectOID(idNumber);
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                oid = odb.getObjectId(sm.getObject());
            }

            if (oid != null)
            {
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long datastoreVersion = odb.ext().getObjectVersion(oid);
                    if (datastoreVersion > 0)
                    {
                        storeMgr.performVersionCheck(sm, Long.valueOf(datastoreVersion), vermd);
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    long ts = odb.ext().getObjectUpdateDate(oid);
                    if (ts > 0)
                    {
                        storeMgr.performVersionCheck(sm, new Timestamp(ts), vermd);
                    }
                }
            }
        }

        // Load any unloaded fields so that DeleteFieldManager has all field values to work with
        sm.loadUnloadedFields();

        try
        {
            // Delete all reachable PC objects (due to dependent-field). Updates lifecycle to P_DELETED
            sm.provideFields(sm.getClassMetaData().getAllMemberPositions(), new DeleteFieldManager(sm));

            // TODO Does NeoDatis cascade delete ALL related objects ? if not then
            // we should delete each object in turn
            if (primaryObject)
            {
                // This delete is for the root object so just persist to NeoDatis and it will delete all dependent objects for us
                long startTime = System.currentTimeMillis();
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("NeoDatis.Delete.Start", 
                        sm.toPrintableID(), sm.getInternalObjectId()));
                }

                if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    long idNumber = ((DatastoreUniqueOID)sm.getInternalObjectId()).getKey();
                    OID oid = new OdbObjectOID(idNumber);
                    if (neodatisDebug)
                    {
                        System.out.println(">> deleteObject odb.deleteObjectWithId(" + oid + ")");
                    }
                    odb.deleteObjectWithId(oid);
                }
                else if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    if (neodatisDebug)
                    {
                        System.out.println(">> deleteObject odb.delete(" + sm.toPrintableID() + ")");
                    }
                    odb.delete(sm.getObject());
                }

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("NeoDatis.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }
                if (storeMgr.getRuntimeManager() != null)
                {
                    storeMgr.getRuntimeManager().incrementDeleteCount();
                }
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    // Debug info about the deleted objects
                    Iterator iter = deleteInfo.objectsList.iterator();
                    while (iter.hasNext())
                    {
                        Object obj = iter.next();
                        ObjectProvider objSM = sm.getExecutionContext().findObjectProvider(obj);
                        if (objSM != null)
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("NeoDatis.Delete.ObjectDeleted", 
                                StringUtils.toJVMIDString(obj), objSM.getInternalObjectId()));
                        }
                    }
                }
            }
        }
        catch (ODBRuntimeException re)
        {
            throw new NucleusDataStoreException("Exception thrown deleting " +
                sm.toPrintableID() + " from datastore", re);
        }
        finally
        {
            if (primaryObject)
            {
                // Clean out the OperationInfo for deletes on this thread
                deleteInfo.objectsList.clear();
                deleteInfo.objectsList = null;
                deleteInfoThreadLocal.remove();
            }
        }
    }*/

    /**
     * Fetches fields of a persistent object from the database.
     * This method is called when we need to load a field. NeoDatis doesn't have a concept of "unloaded" for
     * fields and so we may get this called when accessing an object from HOLLOW state and OM.refresh()
     * is called, or when sm.validate() is called on it when getting it from the L1 cache.
     * @param sm The state manager of the object to be fetched.
     * @param fieldNumbers The numbers of the fields to be fetched.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void fetchObject(ObjectProvider sm, int fieldNumbers[])
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuffer str = new StringBuffer("Fetching object \"");
            str.append(sm.getObjectAsPrintable()).append("\" (id=");
            str.append(sm.getInternalObjectId()).append(")").append(" fields [");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                if (i > 0)
                {
                    str.append(",");
                }
                str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
            }
            str.append("]");
            NucleusLogger.PERSISTENCE.debug(str.toString());
        }

        long startTime = System.currentTimeMillis();
        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_NEODATIS.msg("NeoDatis.Fetch.Start", 
                sm.getObjectAsPrintable(), sm.getInternalObjectId()));
        }

        ExecutionContext ec = sm.getExecutionContext();
        try
        {
            // Check if the object reference is still valid for NeoDatis
            Object pc = findObjectForId(ec, sm.getInternalObjectId());
            if (pc != null && pc != sm.getObject())
            {
                // Object being managed by this ObjectProvider is outdated and needs replacing
                // TODO Localise this
                NucleusLogger.PERSISTENCE.info("Request to populate fields of " + 
                    sm.getObjectAsPrintable() +
                    " but this object is no longer managed by NeoDatis so replacing with " + 
                    StringUtils.toJVMIDString(pc));
                sm.replaceManagedPC(pc);
            }
        }
        catch (ODBRuntimeException re)
        {
            NucleusLogger.PERSISTENCE.warn("Exception fetching fields for object", re);
        }

        // Mark ALL fields as already loaded - NeoDatis doesn't have "unloaded" fields
        sm.replaceFields(sm.getClassMetaData().getAllMemberPositions(), new RetrieveFieldManager(sm));
        updateVersionOfManagedObject(sm);

        if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_NEODATIS.msg("NeoDatis.ExecutionTime", 
                (System.currentTimeMillis() - startTime)));
        }
        if (ec.getStatistics() != null)
        {
            ec.getStatistics().incrementNumReads();
            ec.getStatistics().incrementFetchCount();
        }
    }

    /**
     * Convenience method to retrieve an object from NeoDatis with the specified id.
     * The returned object will be just what NeoDatis returns and no attempt will be made to
     * connect a ObjectProvider if not connected to one
     * @param ec execution context
     * @param id The id
     * @return The object from NeoDatis with this id
     */
    private Object findObjectForId(ExecutionContext ec, Object id)
    {
        Object pc = null;
        if (id instanceof DatastoreUniqueOID)
        {
            // Datastore identity, so grab from NeoDatis via OID
            long idNumber = ((DatastoreUniqueOID)id).getKey();

            ManagedConnection mconn = storeMgr.getConnection(ec);
            OID oid = new OdbObjectOID(idNumber);
            try
            {
                ODB odb = (ODB)mconn.getConnection();
                if (neodatisDebug)
                {
                    System.out.println(">> odb.getObjectFromId(" + oid + ")");
                }
                pc = odb.getObjectFromId(oid);
                if (ec.getStatistics() != null)
                {
                    ec.getStatistics().incrementNumReads();
                }

                if (pc == null)
                {
                    return null;
                }
            }
            catch (ODBRuntimeException re)
            {
                throw new NucleusDataStoreException("Exception thrown finding object with id=" + idNumber, re);
            }
            finally
            {
                mconn.release();
            }
        }
        else
        {
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            String className = storeMgr.getClassNameForObjectID(id, clr, ec);
            AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(className, clr);
            if (acmd.getIdentityType() == IdentityType.APPLICATION)
            {
                // Generate a template object copying the PK values from the id
                Class pcClass = clr.classForName(className, id.getClass().getClassLoader());
                ObjectProvider sm = ec.getNucleusContext().getObjectProviderFactory().newForHollow(ec, pcClass, id);

                // Generate a Criteria query selecting the PK fields
                Class cls = ec.getClassLoaderResolver().classForName(acmd.getFullClassName());
                int[] pkFieldPositions = acmd.getPKMemberPositions();
                ICriterion[] conditions = new ICriterion[pkFieldPositions.length];
                for (int i=0;i<pkFieldPositions.length;i++)
                {
                    Object fieldValue = sm.provideField(pkFieldPositions[i]);
                    String fieldName = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldPositions[i]).getName();
                    conditions[i] = Where.equal(fieldName, fieldValue);
                }
                And whereAnd = Where.and();
                for (int i=0;i<conditions.length;i++)
                {
                    whereAnd.add(conditions[i]);
                }
                CriteriaQuery query = new CriteriaQuery(cls, whereAnd);
                query.setPolymorphic(false);
                ManagedConnection mconn = storeMgr.getConnection(ec);
                ODB odb = (ODB)mconn.getConnection();
                try
                {
                    if (neodatisDebug)
                    {
                        System.out.println(">> findObjectForId odb.getObjects(criteria for " + cls.getName());
                    }
                    Objects results = odb.getObjects(query);
                    if (ec.getStatistics() != null)
                    {
                        ec.getStatistics().incrementNumReads();
                    }

                    if (results != null && results.size() == 1)
                    {
                        pc = results.getFirst();
                    }
                }
                catch (ODBRuntimeException re)
                {
                    throw new NucleusDataStoreException("Exception thrown finding object with id=" + id, re);
                }
                finally
                {
                    mconn.release();
                }
            }
            else
            {
                // Nondurable identity
            }
        }

        return pc;
    }

    /**
     * Accessor for an (at least) hollow persistable object matching the given id.
     * In this sense, the StoreManager may be seen as a kind of level-3-cache. But this methods servers
     * an important purpose: if the StoreManager is managing the in-memory object instantiation 
     * (as part of co-managing the object lifecycle in general), then the StoreManager has to create the object 
     * during this call (if it is not already created. Most relational databases leave the in-memory object 
     * instantiation to DataNucleus, but some object databases may manage the in-memory object instantion.
     *
     * Implementations may simply return null, indicating that they leave the object instantiate to NeoDatis.
     * Other implementations may instantiate the object in question (whether the implementation may trust that 
     * the object is not already instantiated has still to be determined). If an implementation believes that 
     * an object with the given ID should exist, but in fact does not exist, then the implementation should 
     * throw a RuntimeException. It should not silently return null in this case. 
     * @param ec execution context
     * @param id the id of the object in question.
     * @return a Persistable with a valid state (for example: hollow) or null, indicating that the 
     *     implementation leaves the instantiation work to NeoDatis.
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        Object pc = null;
        try
        {
            pc = findObjectForId(ec, id);
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
            }
        }
        catch (NucleusDataStoreException ndse)
        {
            throw new NucleusObjectNotFoundException("Exception thrown finding object with id=" + id, ndse.getNestedExceptions());
        }

        ObjectProvider sm = ec.findObjectProvider(pc);
        if (sm == null)
        {
            // Connect a ObjectProvider to the object (with all fields loaded), and down the object graph
            AbstractClassMetaData acmd =
                ec.getMetaDataManager().getMetaDataForClass(pc.getClass(), ec.getClassLoaderResolver());
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                ODB odb = (ODB)mconn.getConnection();
                sm = NeoDatisUtils.prepareNeoDatisObjectForUse(pc, ec, odb, acmd);
            }
            finally
            {
                mconn.release();
            }
        }

        updateVersionOfManagedObject(sm);

        return pc;
    }

    /**
     * Convenience method to set the version in the ObjectProvider of the object being managed.
     * If the object is not versioned just returns.
     * Goes to NeoDatis and retrieves the version number and sets it in the ObjectProvider.
     * @param sm ObjectProvider managing the object
     */
    private void updateVersionOfManagedObject(ObjectProvider sm)
    {
        if (sm.getClassMetaData().isVersioned())
        {
            ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
            try
            {
                ODB odb = (ODB)mconn.getConnection();
                VersionMetaData vermd = sm.getClassMetaData().getVersionMetaDataForClass();
                OID oid = odb.getObjectId(sm.getObject());
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long datastoreVersion = odb.ext().getObjectVersion(oid, true);
                    if (datastoreVersion > 0)
                    {
                        sm.setTransactionalVersion(Long.valueOf(datastoreVersion));
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    long ts = odb.ext().getObjectUpdateDate(oid, true);
                    if (ts > 0)
                    {
                        sm.setTransactionalVersion(new Timestamp(ts));
                    }
                }
            }
            finally
            {
                mconn.release();
            }
        }
    }

    /**
     * Locates this object in the datastore.
     * @param sm The ObjectProvider for the object to be found 
     * @throws NucleusObjectNotFoundException if the object doesnt exist
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void locateObject(ObjectProvider sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        AbstractClassMetaData cmd = sm.getClassMetaData();

        ManagedConnection mconn = storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            ODB odb = (ODB)mconn.getConnection();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_NEODATIS.msg("NeoDatis.Fetch.Start", 
                    sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            boolean isStored = false;
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Just find if this object is stored
                long idNumber = ((DatastoreUniqueOID)sm.getInternalObjectId()).getKey();
                OID oid = new OdbObjectOID(idNumber);
                try
                {
                    if (neodatisDebug)
                    {
                        System.out.println(">> locateObject odb.getObjectFromId(" + oid + ")");
                    }
                    if (ec.getStatistics() != null)
                    {
                        ec.getStatistics().incrementNumReads();
                    }
                    if (odb.getObjectFromId(oid) != null)
                    {
                        isStored = true;
                    }
                }
                catch (ODBRuntimeException re)
                {
                    throw new NucleusDataStoreException(
                        "Exception thrown locating object with id=" + sm.getInternalObjectId(), re);
                }
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                // Use a Criteria query with AND conditions on al PK fields to find if this exists
                Class cls = ec.getClassLoaderResolver().classForName(sm.getClassMetaData().getFullClassName());
                AbstractClassMetaData acmd = sm.getClassMetaData();
                int[] pkFieldPositions = acmd.getPKMemberPositions();
                ICriterion[] conditions = new ICriterion[pkFieldPositions.length];
                for (int i=0;i<pkFieldPositions.length;i++)
                {
                    Object fieldValue = sm.provideField(pkFieldPositions[i]);
                    String fieldName = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldPositions[i]).getName();
                    conditions[i] = Where.equal(fieldName, fieldValue);
                }
                And whereAnd = Where.and();
                for (int i=0;i<conditions.length;i++)
                {
                    whereAnd.add(conditions[i]);
                }
                CriteriaQuery query = new CriteriaQuery(cls, whereAnd);
                query.setPolymorphic(false);
                try
                {
                    if (neodatisDebug)
                    {
                        System.out.println(">> locateObject odb.getObjects(criteria for " + cls.getName() + ")");
                    }
                    Objects results = odb.getObjects(query);
                    if (ec.getStatistics() != null)
                    {
                        ec.getStatistics().incrementNumReads();
                    }
                    if (results != null && results.size() == 1)
                    {
                        isStored = true;
                    }
                }
                catch (ODBRuntimeException re)
                {
                    throw new NucleusDataStoreException(
                        "Exception thrown locating object with id=" + sm.getInternalObjectId(), re);
                }
            }
            else
            {
                // Nondurable identity
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_NEODATIS.msg("NeoDatis.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }

            if (!isStored)
            {
                throw new NucleusObjectNotFoundException(LOCALISER_NEODATIS.msg("NeoDatis.Object.NotFound", 
                    sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }
        }
        finally
        {
            mconn.release();
        }
    }
}