package org.corfudb.runtime.clients;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.VersionInfo;

import static org.corfudb.protocols.service.CorfuProtocolBase.getPingRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getResetRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getRestartRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getSealRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getVersionRequestMsg;

/**
 * This is a base client which sends basic messages.
 * It mainly sends PINGs, and is also responsible to send
 * SEAL messages used to seal the servers with an epoch.
 *
 * <p>Created by mwei on 12/9/15.
 */
@Slf4j
public class BaseClient extends AbstractClient {

    public BaseClient(IClientRouter router, long epoch, UUID clusterId) {
        super(router, epoch, clusterId);
    }

    /**
     * Ping the endpoint, synchronously.
     * Note: this ping is epoch aware
     *
     * @return True, if the endpoint was reachable, false otherwise.
     */
    public boolean pingSync() {
        try {
            return ping().get();
        } catch (Exception e) {
            log.error("Ping failed due to exception", e);
            return false;
        }
    }

    /**
     * Ping the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint is reachable, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> ping() {
        return sendRequestWithFuture(getPingRequestMsg(), true, true);
    }

    /**
     * Restart the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint restarts successfully, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> restart() {
        return sendRequestWithFuture(getRestartRequestMsg(), true, true);
    }

    /**
     * Reset the endpoint, asynchronously.
     * WARNING: ALL EXISTING DATA ON THIS NODE WILL BE LOST.
     *
     * @return A completable future which will be completed with True if
     * the endpoint resets successfully, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> reset() {
        return sendRequestWithFuture(getResetRequestMsg(), true, true);
    }

    /**
     * Sets the epoch on client router and on the target layout server.
     *
     * @param newEpoch New Epoch to be set
     * @return Completable future which returns true on successful epoch set.
     */
    public CompletableFuture<Boolean> sealRemoteServer(long newEpoch) {
        return sendRequestWithFuture(getSealRequestMsg(newEpoch), false, true);
    }

    /**
     * Get the version info from the target layout server.
     *
     * @return Completable future which returns {@link VersionInfo} object.
     */
    public CompletableFuture<VersionInfo> getVersionInfo() {
        return sendRequestWithFuture(getVersionRequestMsg(), true, true);
    }
}
