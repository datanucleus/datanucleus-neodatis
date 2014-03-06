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

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.neodatis.odb.ODB;
import org.neodatis.odb.ODBFactory;
import org.neodatis.odb.ODBRuntimeException;
import org.neodatis.odb.ODBServer;

/**
 * Implementation of a ConnectionFactory for NeoDatis.
 * Obtains access to the NeoDatis ODB supporting use of 
 * <ul>
 * <li>File-based NeoDatis using a URL like "neodatis:file:{filename}"</li>
 * <li>Embedded server-based NeoDatis using a URL like "neodatis:server:{filename}"</li>
 * <li>Server-based NeoDatis using a URL like "neodatis:host:port/identifier"</li>
 * </ul>
 * <p>
 * When an ODB is obtained for an ExecutionContext, it is cached so all subsequent uses for that
 * ExecutionContext will have the same ODB. This means that the ODB is aligned with the
 * transaction, and so we can use commit/rollback on the ODB. When the txn commits we still have
 * the ODB in the cache so if a subsequent txn  starts on the same ExecutionContext it will then have
 * the same ODB. When the ExecutionContext is closed the ODB is finally closed.
 * </p>
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_NEODATIS = Localiser.getInstance(
        "org.datanucleus.store.neodatis.Localisation", NeoDatisStoreManager.class.getClassLoader());

    /** Use embedded server mode. */
    private boolean neodatisUseEmbeddedServer = false;

    /** Embedded file-based server (if used). */
    private ODBServer neodatisEmbeddedServer = null;

    /** Name of the NeoDatis database file when using a local database. */
    private String neodatisFilename = null;

    /** Hostname where the NeoDatis server is located. */
    private String neodatisHostname = null;

    /** Port on the NeoDatis server. */
    private int neodatisPort = 0;

    /** Identifier when used in client-server mode. */
    private String neodatisIdentifier = null;

    /** Cache of the NeoDatis ODB keyed by ExecutionContext. */
    protected HashMap odbByExecutionContext = new HashMap();

    /**
     * Constructor
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        String url = storeMgr.getConnectionURL();
        if (!url.startsWith("neodatis"))
        {
            throw new NucleusException(LOCALISER_NEODATIS.msg("NeoDatis.URLInvalid", url));
        }

        // Split the URL into filename, or "host:port"
        String neodatisStr = url.substring(9); // Omit the "neodatis" prefix
        if (neodatisStr.indexOf("server:") < 0 && neodatisStr.indexOf("file:") < 0)
        {
            // TCP/IP Server
            neodatisHostname = neodatisStr.substring(0, neodatisStr.indexOf(':'));
            try
            {
                String str = neodatisStr.substring(neodatisStr.indexOf(':') + 1);
                String portName = str;
                if (str.indexOf("/") > 0)
                {
                    portName = str.substring(0, str.indexOf("/"));
                    neodatisIdentifier = str.substring(str.indexOf("/") + 1);
                }
                neodatisPort = Integer.valueOf(portName).intValue();
            }
            catch (NumberFormatException nfe)
            {
                throw new NucleusUserException(LOCALISER_NEODATIS.msg("NeoDatis.URLInvalid", url));
            }
        }
        else
        {
            // Local file, or Embedded Server
            String filename = neodatisStr.substring(neodatisStr.indexOf(':') + 1); // Omit the "file:"/"server:"
            try
            {
                // Try absolute filenames, taking it as a URL
                neodatisFilename = new File(new URL(neodatisStr).toURI()).getAbsolutePath();
            }
            catch (Exception e)
            {
                try
                {
                    // Try as relative to ${user.dir}
                    String absFilename = System.getProperty("user.dir") + System.getProperty("file.separator") + filename;
                    File file = new File(absFilename);
                    NucleusLogger.CONNECTION.info(LOCALISER_NEODATIS.msg("NeoDatis.FilestoreRelativePath", neodatisStr, absFilename));
                    neodatisFilename = file.getAbsolutePath();
                }
                catch (Exception e2)
                {
                    throw new NucleusUserException(LOCALISER_NEODATIS.msg("NeoDatis.FilenameError", 
                        neodatisFilename, e.getMessage()), e);
                }
            }
            neodatisUseEmbeddedServer = neodatisStr.startsWith("server:");
        }
    }

    /**
     * Convenience method to notify the factory that the specified ExecutionContext is closing, so to close
     * any resources for it.
     * @param poolKey Key for the connection pool
     */
    public void closeODBForExecutionContext(Object poolKey)
    {
        Object obj = odbByExecutionContext.get(poolKey);
        if (obj != null)
        {
            ODB odb = (ODB)obj;
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug(LOCALISER_NEODATIS.msg("NeoDatis.ClosingConnection", 
                    storeMgr.getConnectionURL(), StringUtils.toJVMIDString(odb)));
            }
            try
            {
                odb.close();
            }
            catch (Exception e)
            {
                // TODO Log this or throw exception
            }
            odbByExecutionContext.remove(poolKey);
        }
        if (odbByExecutionContext.isEmpty() && neodatisEmbeddedServer != null)
        {
            try
            {
                neodatisEmbeddedServer.close();
                neodatisEmbeddedServer = null;
            }
            catch (Exception e)
            {
                NucleusLogger.CONNECTION.warn("Exception closing the NeoDatis server", e);
            }
        }
    }

    /**
     * Convenience method to create a new ODB for this datastore.
     * @return The ODB
     */
    protected ODB getNewODB()
    {
        ODB odb = null;
        if (neodatisFilename != null)
        {
            // NeoDatis using local file
            try
            {
                if (neodatisUseEmbeddedServer)
                {
                    // Embedded Server
                    if (neodatisEmbeddedServer == null)
                    {
                        neodatisEmbeddedServer = ODBFactory.openServer(10001);
                    }
                    // TODO Cater for username/password when NeoDatis allows the option
                    odb = neodatisEmbeddedServer.openClient(neodatisFilename);
                }
                else
                {
                    // Local file
                    String username = storeMgr.getConnectionUserName();
                    if (username != null)
                    {
                        // Encrypted file
                        odb = ODBFactory.open(neodatisFilename,
                            username, storeMgr.getConnectionPassword());
                    }
                    else
                    {
                        // Unencrypted file
                        odb = ODBFactory.open(neodatisFilename);
                    }
                }
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(LOCALISER_NEODATIS.msg("NeoDatis.OpeningConnection", 
                        storeMgr.getConnectionURL(), StringUtils.toJVMIDString(odb)));
                }
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(LOCALISER_NEODATIS.msg("NeoDatis.ConnectionError", 
                    storeMgr.getConnectionURL()), e);
            }
        }
        else
        {
            // NeoDatis using client connecting to server at hostname:port
            try
            {
                odb = ODBFactory.openClient(neodatisHostname, neodatisPort, neodatisIdentifier,
                    storeMgr.getConnectionUserName(),
                    storeMgr.getConnectionPassword());
                if (NucleusLogger.CONNECTION.isDebugEnabled())
                {
                    NucleusLogger.CONNECTION.debug(LOCALISER_NEODATIS.msg("NeoDatis.OpeningConnection",
                        storeMgr.getConnectionURL(), StringUtils.toJVMIDString(odb)));
                }
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(LOCALISER_NEODATIS.msg("NeoDatis.ConnectionError", 
                    storeMgr.getConnectionURL()), e);
            }
        }

        if (NeoDatisPersistenceHandler.neodatisDebug)
        {
            System.out.println("ODBFactory.open returns " + StringUtils.toJVMIDString(odb));
        }
        return odb;
    }

    /**
     * Method to create a new managed connection.
     * @param ec Key for the pool
     * @param txnOptionsIgnored Transaction options
     * @return The connection
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map txnOptionsIgnored)
    {
        return new ManagedConnectionImpl(ec);
    }

    /**
     * Implementation of a ManagedConnection for NeoDatis.
     */
    class ManagedConnectionImpl extends AbstractManagedConnection
    {
        Object poolKey = null;

        /**
         * Constructor.
         * @param storeMgr
         * @param transactionOptions Any options
         */
        ManagedConnectionImpl(Object poolKey)
        {
            this.poolKey = poolKey;
        }

        /**
         * Obtain a XAResource which can be enlisted in a transaction
         */
        public XAResource getXAResource()
        {
            ODB odb = (ODB)getConnection();
            return new EmulatedXAResource(odb);
        }

        /**
         * Create a connection to the resource
         */
        public Object getConnection()
        {
            if (conn == null)
            {
                if (poolKey != null)
                {
                    Object objCont = odbByExecutionContext.get(poolKey);
                    if (objCont != null)
                    {
                        conn = objCont;
                        return conn;
                    }
                }

                conn = getNewODB();
                if (poolKey != null)
                {
                    odbByExecutionContext.put(poolKey, conn);

                    // Register the ODB as active for this StoreManager
                    ((NeoDatisStoreManager) storeMgr).registerODB((ODB)conn);
                }
            }
            return conn;
        }

        /**
         * Close the connection
         */
        public void close()
        {
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPreClose();
            }
            try
            {
                if (conn != null)
                {
                    if (NucleusLogger.CONNECTION.isDebugEnabled())
                    {
                        NucleusLogger.CONNECTION.debug(LOCALISER_NEODATIS.msg("NeoDatis.CommittingConnection", 
                            storeMgr.getConnectionURL(), StringUtils.toJVMIDString(conn)));
                    }

                    // This is for when not enlisted in a TXN manager (non-tx read/write etc)
                    // TODO Would be nice to only do it then since XAResource is committed further down
                    if (NeoDatisPersistenceHandler.neodatisDebug)
                    {
                        System.out.println("odb.commit " + StringUtils.toJVMIDString(conn));
                    }
                    ((ODB)conn).commit();

                    // Deregister the ODB for this StoreManager
                    if (poolKey != null)
                    {
                        ((NeoDatisStoreManager)storeMgr).deregisterODB((ODB)conn);
                    }

                    // Close the ODB since NeoDatis caches objects
                    // TODO When we have a workaround for the NeoDatis caching remove these 3 lines
                    if (NeoDatisPersistenceHandler.neodatisDebug)
                    {
                        System.out.println("odb.close " + StringUtils.toJVMIDString(conn));
                    }
                    ((ODB)conn).close();
                    conn = null;
                    if (poolKey != null)
                    {
                        odbByExecutionContext.remove(poolKey);
                    }
                }
            }
            catch (ODBRuntimeException re)
            {
                throw new NucleusDataStoreException("Exception thrown during close of ManagedConnection", re);
            }
            try
            {
                for( int i=0; i<listeners.size(); i++ )
                {
                    ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
                }
            }
            finally
            {
                listeners.clear();
            }
            if (NucleusLogger.CONNECTION.isDebugEnabled())
            {
                NucleusLogger.CONNECTION.debug("The connection has been closed : "+this);
            }
        }
    }

    /**
     * Emulate the two phase protocol for non XA, wrapping the NeoDatis ODB.
     */
    class EmulatedXAResource implements XAResource
    {
        ODB odb;

        EmulatedXAResource(ODB conn)
        {
            this.odb = conn;
        }

        public void commit(Xid arg0, boolean arg1) throws XAException
        {
            try
            {
                // Commit the ODB
                if (NeoDatisPersistenceHandler.neodatisDebug)
                {
                    System.out.println("odb.commit " + StringUtils.toJVMIDString(odb));
                }
                odb.commit();
            }
            catch (ODBRuntimeException re)
            {
                throw new XAException("Exception thrown when committing " + re.getMessage());
            }
        }

        public void end(Xid arg0, int arg1) throws XAException
        {
            //ignore
        }

        public void forget(Xid arg0) throws XAException
        {
            //ignore
        }

        public int getTransactionTimeout() throws XAException
        {
            return 0;
        }

        public boolean isSameRM(XAResource arg0) throws XAException
        {
            if (arg0 instanceof EmulatedXAResource)
            {
                if (odb.equals(((EmulatedXAResource)arg0).odb))
                {
                    return true;
                }
            }
            return false;
        }

        public int prepare(Xid arg0) throws XAException
        {
            return 0;
        }

        public Xid[] recover(int arg0) throws XAException
        {
            throw new XAException("Unsupported operation");
        }

        public void rollback(Xid arg0) throws XAException
        {
            try
            {
                // Rollback the ODB
                if (NeoDatisPersistenceHandler.neodatisDebug)
                {
                    System.out.println("odb.rollback " + StringUtils.toJVMIDString(odb));
                }
                odb.rollback();
            }
            catch (ODBRuntimeException re)
            {
                throw new XAException("Exception thrown when rolling back " + re.getMessage());
            }
        }

        public boolean setTransactionTimeout(int arg0) throws XAException
        {
            return false;
        }

        public void start(Xid arg0, int arg1) throws XAException
        {
            //ignore
        }        
    }
}