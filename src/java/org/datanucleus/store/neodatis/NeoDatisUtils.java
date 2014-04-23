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

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.neodatis.fieldmanager.AssignObjectProviderFieldManager;
import org.neodatis.odb.ODB;

/**
 * Utilities for NeoDatis (http://www.neodatis.org).
 */
public class NeoDatisUtils
{
    /**
     * Convenience method to take an object returned by NeoDatis (from a query for example), and prepare 
     * it for passing to the user. Makes sure there is a ObjectProvider connected, with all fields 
     * marked as loaded.
     * @param obj The object (from NeoDatis)
     * @param ec execution context
     * @param odb ODB that returned the object
     * @param cmd ClassMetaData for the object
     * @return The ObjectProvider for this object
     */
    public static ObjectProvider prepareNeoDatisObjectForUse(Object obj, ExecutionContext ec, 
            ODB odb, AbstractClassMetaData cmd)
    {
        if (!ec.getApiAdapter().isPersistable(obj))
        {
            return null;
        }

        ObjectProvider sm = ec.findObjectProvider(obj);
        if (sm == null)
        {
            // Find the identity
            Object id = getIdentityForObject(obj, cmd, ec, odb);

            // Object not managed so give it a ObjectProvider before returning it
            // This marks all fields as loaded (which they are with NeoDatis)
            sm = ec.getNucleusContext().getObjectProviderFactory().newForPersistentClean(ec, id, obj);

            // Assign ObjectProviders down the object graph
            sm.provideFields(cmd.getAllMemberPositions(), new AssignObjectProviderFieldManager(sm, odb));
        }

        // Wrap all unwrapped SCO fields of this instance so we can pick up any changes
        sm.replaceAllLoadedSCOFieldsWithWrappers();

        return sm;
    }

    /**
     * Convenience method to return the (DataNucleus) identity for an object.
     * @param obj The object
     * @param acmd MetaData for the object
     * @param ec execution context
     * @param odb ODB to use
     * @return The DataNucleus identity
     */
    public static Object getIdentityForObject(Object obj, AbstractClassMetaData acmd, ExecutionContext ec,
            ODB odb)
    {
        if (acmd.getIdentityType() == IdentityType.DATASTORE)
        {
            long datastoreId = odb.getObjectId(obj).getObjectId();
            return ec.getNucleusContext().getIdentityManager().getDatastoreId(datastoreId);
        }
        else if (acmd.getIdentityType() == IdentityType.APPLICATION)
        {
            return ec.getNucleusContext().getIdentityManager().getApplicationId(obj, acmd);
        }
        else
        {
            // Nondurable identity
            return null;
        }
    }
}