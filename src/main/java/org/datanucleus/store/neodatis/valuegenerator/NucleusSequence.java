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
package org.datanucleus.store.neodatis.valuegenerator;

/**
 * Representation of a sequence for either a class or field.
 * Object is stored in NeoDatis storing the class/field name and the current sequence value.
 * Works in a similar way to SequenceTable for RDBMS in that an object is persisted for
 * which class or field that needs sequence values and when a new one is required the object
 * is retrieved and the sequence value updated.
 */
public class NucleusSequence
{
    /** Name of the class or field. Acts as the primary key of the object in NeoDatis. */
    private String entityName;

    /** Current sequence value. */
    private long currentValue = 0;

    /**
     * Constructor.
     * @param entity The class/field name that the sequence value is for
     */
    public NucleusSequence(String entity)
    {
        this.entityName = entity;
    }

    /**
     * Accessor for the current value.
     * @return Current value
     */
    public long getCurrentValue()
    {
        return currentValue;
    }

    /**
     * Accessor for the entity name
     * @return Entity (class/field) name
     */
    public String getEntityName()
    {
        return entityName;
    }

    /**
     * Mutator for current value
     * @param value The current value for this sequence.
     */
    public void setCurrentValue(long value)
    {
        this.currentValue = value;
    }

    /**
     * Method to update the current value.
     * TODO Think about changing this so we pass in the increment value in the constructor and just have an 
     * increment() method.
     * @param increment The amount to increment by (must be positive)
     */
    public void incrementCurrentValue(long increment)
    {
        if (increment <= 0)
        {
            return;
        }
        currentValue += increment;
    }
}