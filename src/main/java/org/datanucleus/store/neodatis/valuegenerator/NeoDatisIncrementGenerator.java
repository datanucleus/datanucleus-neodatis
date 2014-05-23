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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerationException;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.Localiser;
import org.neodatis.odb.ODB;
import org.neodatis.odb.Objects;
import org.neodatis.odb.core.query.criteria.Where;
import org.neodatis.odb.impl.core.query.criteria.CriteriaQuery;

/**
 * Value generator for NeoDatis that provides increment/sequence like generation.
 * Utilises the persistence of objects of NucleusSequence. 
 * Each objects stores the sequence class/field name and the current value of that sequence.
 */
public class NeoDatisIncrementGenerator extends AbstractDatastoreGenerator<Long>
{
    /** The NeoDatis ODB to use for generating values */
    private ODB odb = null;

    /** Name of the sequence that we are storing values under (name of the class/field). */
    private final String sequenceName;

    /**
     * Constructor.
     * @param name Symbolic name for this generator
     * @param props Properties defining the behaviour of this generator
     */
    public NeoDatisIncrementGenerator(String name, Properties props)
    {
        super(name, props);
        allocationSize = 5;

        // TODO Check these names and what we want to use for NeoDatis (classname or fieldname)
        if (properties.getProperty("sequence-name") != null)
        {
            // Specified sequence-name so use that
            sequenceName = properties.getProperty("sequence-name");
        }
        else if (properties.getProperty("field-name") != null)
        {
            // Use field name as the sequence name so we have one sequence per field on the class
            sequenceName = properties.getProperty("field-name");
        }
        else
        {
            // Use actual class name as the sequence name so we have one sequence per class
            sequenceName = properties.getProperty("class-name");
        }
    }

    /**
     * Get a new PoidBlock with the specified number of ids.
     * @param number The number of additional ids required
     * @return the PoidBlock
     */
    protected ValueGenerationBlock<Long> obtainGenerationBlock(int number)
    {
        ValueGenerationBlock<Long> block = null;

        // Try getting the block
        try
        {
            odb = (ODB)connectionProvider.retrieveConnection().getConnection();

            try
            {
                if (number < 0)
                {
                    block = reserveBlock();
                }
                else
                {
                    block = reserveBlock(number);
                }
            }
            catch (ValueGenerationException poidex)
            {
                NucleusLogger.VALUEGENERATION.info(Localiser.msg("040003", poidex.getMessage()));
                throw poidex;
            }
            catch (RuntimeException ex)
            {
                // exceptions cached by the poid should be enclosed in PoidException
                // when the exceptions are not catched exception by poid, we give a new try
                // in creating the repository
                NucleusLogger.VALUEGENERATION.info(Localiser.msg("040003", ex.getMessage()));
                throw ex;
            }
        }
        finally
        {
            if (odb != null)
            {
                connectionProvider.releaseConnection();
                odb = null;
            }
        }

        return block;
    }

    /**
     * Method to reserve a block of "size" identities.
     * @param size Block size
     * @return The reserved block
     */
    protected ValueGenerationBlock<Long> reserveBlock(long size)
    {
        List<Long> ids = new ArrayList<Long>();

        // Find the current NucleusSequence object in NeoDatis for this sequence
        NucleusSequence seq = null;
        NucleusSequence baseSeq = new NucleusSequence(sequenceName);
        CriteriaQuery query = new CriteriaQuery(NucleusSequence.class, 
            Where.equal("entityName", sequenceName));
        Objects results = null;
        try
        {
            results = odb.getObjects(query);
        }
        catch (Exception e)
        {
            // Exception thrown in getting sequence object
            NucleusLogger.PERSISTENCE.error("Exception thrown getting value for sequence " + sequenceName, e);
        }
        if (results != null && results.size() == 1)
        {
            seq = (NucleusSequence)results.next();
        }

        Long nextVal = null;
        if (seq == null)
        {
            seq = baseSeq;
            nextVal = Long.valueOf(1);
            seq.setCurrentValue(1);
        }
        else
        {
            nextVal = Long.valueOf(seq.getCurrentValue());
        }
        seq.incrementCurrentValue(allocationSize);
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE.debug(Localiser.msg("Neodatis.ValueGenerator.UpdatingSequence", sequenceName, "" + seq.getCurrentValue()));
        }

        for (int i=0; i<size; i++)
        {
            ids.add(nextVal);
            nextVal = Long.valueOf(nextVal.longValue()+1);
        }

        // Update the NucleusSequence object in NeoDatis
        try
        {
            odb.store(seq);
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Exception thrown updating sequence in NeoDatis for name=" + sequenceName, e);
        }

        return new ValueGenerationBlock<Long>(ids);
    }
}