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
package org.datanucleus.store.neodatis.fieldmanager;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.neodatis.NeoDatisUtils;
import org.neodatis.odb.ODB;

/**
 * Field manager that starts from the source object and for all fields will assign ObjectProviders to all
 * related PersistenceCapable objects found (unless already managed), assuming they are in P_CLEAN state.
 **/
public class AssignObjectProviderFieldManager extends AbstractFieldManager
{
    /** ObjectProvider for the owning object whose fields are being fetched. */
    private final ObjectProvider op;

    /** NeoDatis ODB to use when assigning any ObjectProviders down the object graph. */
    private final ODB odb;

    /**
     * Constructor.
     * @param op Object Provider for the object.
     * @param odb Neodatis ODB
     */
    public AssignObjectProviderFieldManager(ObjectProvider op, ODB odb)
    {
        this.op = op;
        this.odb = odb;
    }

    /**
     * Utility method to process the passed persistable object.
     * We know that this object has no ObjectProvider when it comes in here.
     * @param fieldNumber Absolute field number
     * @param pc The persistable object
     */
    protected void processPersistable(int fieldNumber, Object pc)
    {
        ExecutionContext ec = op.getExecutionContext();
        ObjectProvider theSM = null;

        // No ObjectProvider but object returned to us by NeoDatis so we know it is persistent
        // Connect a ObjectProvider in P_CLEAN state with all fields loaded
        AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(pc.getClass(), 
            ec.getClassLoaderResolver());
        Object id = NeoDatisUtils.getIdentityForObject(pc, acmd, ec, odb);
        theSM = ec.newObjectProviderForPersistentClean(id, pc);

        // Recurse to all fields of this object since just assigned its ObjectProvider
        theSM.provideFields(theSM.getClassMetaData().getAllMemberPositions(), 
            new AssignObjectProviderFieldManager(theSM, odb));
    }

    /**
     * Method to store an object field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeObjectField(int fieldNumber, Object value)
    {
        if (value != null)
        {
            ExecutionContext ec = op.getExecutionContext();
            ApiAdapter api = ec.getApiAdapter();
            AbstractMemberMetaData fmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            if (api.isPersistable(value))
            {
                // PC field
                ObjectProvider valueSM = ec.findObjectProvider(value);
                if (valueSM == null)
                {
                    // Field is not yet managed
                    processPersistable(fieldNumber, value);
                }
            }
            else if (value instanceof Collection)
            {
                // Collection that may contain PCs
                if (fmd.hasCollection() && fmd.getCollection().elementIsPersistent())
                {
                    Collection coll = (Collection)value;
                    Iterator iter = coll.iterator();
                    while (iter.hasNext())
                    {
                        Object element = iter.next();
                        if (api.isPersistable(element))
                        {
                            ObjectProvider elementSM = op.getExecutionContext().findObjectProvider(element);
                            if (elementSM == null)
                            {
                                // Collection Element is not yet managed
                                processPersistable(fieldNumber, element);
                            }
                        }
                    }
                }
            }
            else if (value instanceof Map)
            {
                // Map that may contain PCs in key or value
                if (fmd.hasMap())
                {
                    if (fmd.getMap().keyIsPersistent())
                    {
                        Map map = (Map)value;
                        Set keys = map.keySet();
                        Iterator iter = keys.iterator();
                        while (iter.hasNext())
                        {
                            Object mapKey = iter.next();
                            if (api.isPersistable(mapKey))
                            {
                                ObjectProvider keySM = op.getExecutionContext().findObjectProvider(mapKey);
                                if (keySM == null)
                                {
                                    // Map Key is not yet managed
                                    processPersistable(fieldNumber, mapKey);
                                }
                            }
                        }
                    }
                    if (fmd.getMap().valueIsPersistent())
                    {
                        Map map = (Map)value;
                        Collection values = map.values();
                        Iterator iter = values.iterator();
                        while (iter.hasNext())
                        {
                            Object mapValue = iter.next();
                            if (api.isPersistable(mapValue))
                            {
                                ObjectProvider valueSM = op.getExecutionContext().findObjectProvider(mapValue);
                                if (valueSM == null)
                                {
                                    // Map Value is not yet managed
                                    processPersistable(fieldNumber, mapValue);
                                }
                            }
                        }
                    }
                }
            }
            else if (value instanceof Object[])
            {
                // Array that may contain PCs
                if (fmd.hasArray() && fmd.getArray().elementIsPersistent())
                {
                    for (int i=0;i<Array.getLength(value);i++)
                    {
                        Object element = Array.get(value, i);
                        if (api.isPersistable(element))
                        {
                            ObjectProvider elementSM = op.getExecutionContext().findObjectProvider(element);
                            if (elementSM == null)
                            {
                                // Array element is not yet managed
                                processPersistable(fieldNumber, element);
                            }
                        }
                    }
                }
            }
            else
            {
                // Primitive, or primitive array, or some unsupported container type
            }
        }
    }

    /**
     * Method to store a boolean field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        // Do nothing
    }

    /**
     * Method to store a byte field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeByteField(int fieldNumber, byte value)
    {
        // Do nothing
    }

    /**
     * Method to store a char field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeCharField(int fieldNumber, char value)
    {
        // Do nothing
    }

    /**
     * Method to store a double field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeDoubleField(int fieldNumber, double value)
    {
        // Do nothing
    }

    /**
     * Method to store a float field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeFloatField(int fieldNumber, float value)
    {
        // Do nothing
    }

    /**
     * Method to store an int field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeIntField(int fieldNumber, int value)
    {
        // Do nothing
    }

    /**
     * Method to store a long field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeLongField(int fieldNumber, long value)
    {
        // Do nothing
    }

    /**
     * Method to store a short field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeShortField(int fieldNumber, short value)
    {
        // Do nothing
    }

    /**
     * Method to store a string field.
     * @param fieldNumber Number of the field (absolute)
     * @param value Value of the field
     */
    public void storeStringField(int fieldNumber, String value)
    {
        // Do nothing
    }
}