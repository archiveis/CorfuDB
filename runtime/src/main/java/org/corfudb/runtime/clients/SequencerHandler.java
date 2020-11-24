package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.service.CorfuProtocolSequencer;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.StreamsAddressResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.TokenResponseMsg;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

/**
 * A sequencer handler client.
 * This client handles the token responses from the sequencer server.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 */
public class SequencerHandler implements IClient, IHandler<SequencerClient> {

    @Setter
    @Getter
    IClientRouter router;

    @Override
    public SequencerClient getClient(long epoch, UUID clusterID) {
        return new SequencerClient(router, epoch, clusterID);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter(onMethod_={@Override})
    @Deprecated
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * For old CorfuMsg, use {@link #msgHandler}
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

    /**
     * Handle a token response from the server.
     *
     * @param msg The token response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link TokenResponseMsg} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.TOKEN_RESPONSE)
    private static Object handleTokenResponse(CorfuMessage.ResponseMsg msg, ChannelHandlerContext ctx,
                                              IClientRouter r) {
        TokenResponseMsg responseMsg = msg.getPayload().getTokenResponse();

        return CorfuProtocolSequencer.getTokenResponse(responseMsg);
    }

    /**
     * Handle a bootstrap sequencer response from the server.
     *
     * @param msg The bootstrap sequencer response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.BOOTSTRAP_SEQUENCER_RESPONSE)
    private static Object handleBootstrapSequencerResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                           IClientRouter r) {
        BootstrapSequencerResponseMsg responseMsg = msg.getPayload().getBootstrapSequencerResponse();
        BootstrapSequencerResponseMsg.Type type = responseMsg.getRespType();

        switch (type) {
            case ACK:   return true;
            case NACK:  return false;
            // TODO INVALID
            default:    throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    /**
     * Handle a sequencer trim response from the server.
     *
     * @param msg The sequencer trim response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return Always True, since the sequencer trim was successful.
     */
    @ResponseHandler(type = PayloadCase.SEQUENCER_TRIM_RESPONSE)
    private static Object handleSequencerTrimResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientRouter r) {
        return true;
    }

    /**
     * Handle a sequencer metrics response from the server.
     *
     * @param msg The sequencer metrics response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link SequencerMetrics} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.SEQUENCER_METRICS_RESPONSE)
    private static Object handleSequencerMetricsResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                         IClientRouter r) {
        SequencerMetricsResponseMsg responseMsg = msg.getPayload().getSequencerMetricsResponse();
        SequencerMetricsMsg sequencerMetricsMsg = responseMsg.getSequencerMetrics();

        return CorfuProtocolCommon.getSequencerMetrics(sequencerMetricsMsg);
    }

    /**
     * Handle a streams address response from the server.
     *
     * @param msg The streams address response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link StreamsAddressResponse} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.STREAMS_ADDRESS_RESPONSE)
    private static Object handleStreamsAddressResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                       IClientRouter r) {
        StreamsAddressResponseMsg responseMsg = msg.getPayload().getStreamsAddressResponse();

        return CorfuProtocolCommon.getStreamsAddressResponse(responseMsg.getLogTail(), responseMsg.getAddressMapList());
    }
}
