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

import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;

/**
 * FieldManager to handle the retrieval of an object from NeoDatis, and marks all fields as loaded
 * since NeoDatis doesn't currently have the concept of not loaded.
 * This really should never get called since all fields should always be loaded. Exists to catch
 * any situations where a field is marked as not loaded during the persistence lifecycle.
 */
public class RetrieveFieldManager extends AbstractFieldManager
{
    /** ObjectProvider of the owning object whose fields are being fetched. */
    private ObjectProvider op;

    public RetrieveFieldManager(ObjectProvider sm)
    {
        this.op = sm;
    }

    public Object fetchObjectField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        Object value = sfv.fetchObjectField(fieldNumber);
        return value;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchBooleanField(fieldNumber);
    }

    public byte fetchByteField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchByteField(fieldNumber);
    }

    public char fetchCharField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchCharField(fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchDoubleField(fieldNumber);
    }

    public float fetchFloatField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchFloatField(fieldNumber);
    }

    public int fetchIntField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchIntField(fieldNumber);
    }

    public long fetchLongField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchLongField(fieldNumber);
    }

    public short fetchShortField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchShortField(fieldNumber);
    }

    public String fetchStringField(int fieldNumber)
    {
        SingleValueFieldManager sfv = new SingleValueFieldManager();
        op.provideFields(new int[]{fieldNumber}, sfv);
        return sfv.fetchStringField(fieldNumber);
    }
}