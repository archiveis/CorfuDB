package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getCommitLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getPrepareLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getProposeLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class CorfuLayoutHandlerTest {

    // The BaseHandler instance used for testing
    private LayoutHandler layoutHandler;

    // Objects that need to be mocked
    private IClientRouter mockClientRouter;
    private ChannelHandlerContext mockChannelHandlerContext;

    private final AtomicInteger requestCounter = new AtomicInteger();

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     *
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(boolean ignoreClusterId, boolean ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * Perform the required preparation before running individual
     * tests by preparing the mocks.
     */
    @Before
    public void setup() {
        mockClientRouter = mock(IClientRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        layoutHandler = new LayoutHandler();
        layoutHandler.setRouter(mockClientRouter);
    }

    /**
     * Test that the LayoutHandler correctly handles a LAYOUT_RESPONSE.
     */
    @Test
    public void testGetLayout() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        List<String> unresponsiveServers = defaultLayout.getUnresponsiveServers();
        unresponsiveServers.add("localhost:9001");
        defaultLayout.setUnresponsiveServers(unresponsiveServers);

        ResponseMsg response = getResponseMsg(
                getBasicHeader(false, true),
                getLayoutResponseMsg(defaultLayout)
        );

        layoutHandler.handleMessage(response, mockChannelHandlerContext);
        ArgumentCaptor<Layout> layoutCaptor = ArgumentCaptor.forClass(Layout.class);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), layoutCaptor.capture());

        Layout layout = layoutCaptor.getValue();
        assertThat(layout.getActiveLayoutServers()).containsExactly("localhost:9000", "localhost:9002");
        assertThat(layout.getSequencers()).containsExactly("localhost:9000");
        assertThat(layout.getAllLogServers()).containsExactly("localhost:9002", "localhost:9001", "localhost:9000");
    }

    /**
     * Test that the LayoutHandler correctly handles a BOOTSTRAP_LAYOUT_RESPONSE.
     */
    @Test
    public void testBootstrapLayout() {
        ResponseMsg responseACK = getResponseMsg(
                getBasicHeader(false, true),
                getBootstrapLayoutResponseMsg(true)
        );

        ResponseMsg responseNACK = getResponseMsg(
                getBasicHeader(false, false),
                getBootstrapLayoutResponseMsg(false)
        );

        layoutHandler.handleMessage(responseACK, mockChannelHandlerContext);
        layoutHandler.handleMessage(responseNACK, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(responseACK.getHeader().getRequestId(), true);
        verify(mockClientRouter).completeRequest(responseNACK.getHeader().getRequestId(), false);
    }

    /**
     * Test that the LayoutHandler correctly handles a PREPARE_LAYOUT_RESPONSE.
     */
    @Test
    public void testPrepare() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        long defaultRank = 0L;
        ResponseMsg responseACK = getResponseMsg(
                getBasicHeader(false, true),
                getPrepareLayoutResponseMsg(true, defaultRank, defaultLayout)
        );
        ResponseMsg responseREJECT = getResponseMsg(
                getBasicHeader(false, false),
                getPrepareLayoutResponseMsg(false, defaultRank, defaultLayout)
        );

        // Verify that the correct request was completed (once) with the appropriate value.
        layoutHandler.handleMessage(responseACK, mockChannelHandlerContext);
        ArgumentCaptor<LayoutPrepareResponse> layoutPrepareCaptor = ArgumentCaptor.forClass(LayoutPrepareResponse.class);
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(responseACK.getHeader().getRequestId()), layoutPrepareCaptor.capture());

        LayoutPrepareResponse layoutPrepareCaptorValue = layoutPrepareCaptor.getValue();
        Layout retLayout = layoutPrepareCaptorValue.getLayout();
        assertThat(retLayout.getActiveLayoutServers()).containsExactly("localhost:9000", "localhost:9001", "localhost:9002");
        assertThat(retLayout.getSequencers()).containsExactly("localhost:9000");
        assertThat(retLayout.getAllLogServers()).containsExactly("localhost:9002", "localhost:9001", "localhost:9000");

        // Verify that the correct exception was thrown with the appropriate field set.
        layoutHandler.handleMessage(responseREJECT, mockChannelHandlerContext);
        ArgumentCaptor<OutrankedException> exceptionCaptor = ArgumentCaptor.forClass(OutrankedException.class);
        verify(mockClientRouter).completeExceptionally(
                eq(responseREJECT.getHeader().getRequestId()), exceptionCaptor.capture());
        OutrankedException outrankedException = exceptionCaptor.getValue();
        assertThat(outrankedException.getNewRank()).isEqualTo(defaultRank);

        retLayout = outrankedException.getLayout();
        assertThat(retLayout.getActiveLayoutServers()).containsExactly("localhost:9000", "localhost:9001", "localhost:9002");
        assertThat(retLayout.getSequencers()).containsExactly("localhost:9000");
        assertThat(retLayout.getAllLogServers()).containsExactly("localhost:9002", "localhost:9001", "localhost:9000");
    }

    /**
     * Test that the LayoutHandler correctly handles a PROPOSE_LAYOUT_RESPONSE.
     */
    @Test
    public void testPropose() {
        long defaultRank = 0L;
        ResponseMsg responseACK = getResponseMsg(
                getBasicHeader(false, false),
                getProposeLayoutResponseMsg(true, defaultRank)
        );
        ResponseMsg responseREJECT = getResponseMsg(
                getBasicHeader(false, false),
                getProposeLayoutResponseMsg(false, defaultRank)
        );

        // Verify that the correct request was completed (once) with the appropriate value.
        layoutHandler.handleMessage(responseACK, mockChannelHandlerContext);
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(responseACK.getHeader().getRequestId(), true);

        // Verify that the correct exception was thrown with the appropriate field set.
        layoutHandler.handleMessage(responseREJECT, mockChannelHandlerContext);
        ArgumentCaptor<OutrankedException> exceptionCaptor = ArgumentCaptor.forClass(OutrankedException.class);
        verify(mockClientRouter).completeExceptionally(
                eq(responseREJECT.getHeader().getRequestId()), exceptionCaptor.capture());
        OutrankedException outrankedException = exceptionCaptor.getValue();
        assertThat(outrankedException.getNewRank()).isEqualTo(defaultRank);
    }

    /**
     * Test that the LayoutHandler correctly handles a COMMIT_LAYOUT_RESPONSE.
     */
    @Test
    public void testCommit() {
        ResponseMsg responseACK = getResponseMsg(
                getBasicHeader(false, true),
                getCommitLayoutResponseMsg(true)
        );

        ResponseMsg responseNACK = getResponseMsg(
                getBasicHeader(false, false),
                getCommitLayoutResponseMsg(false)
        );

        layoutHandler.handleMessage(responseACK, mockChannelHandlerContext);
        layoutHandler.handleMessage(responseNACK, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(responseACK.getHeader().getRequestId(), true);
        verify(mockClientRouter).completeRequest(responseNACK.getHeader().getRequestId(), false);
    }

    /* Helper */
    private Layout getDefaultLayout() throws IOException {
        String JSONDefaultLayout = new String(Files.readAllBytes(
                Paths.get("src/test/resources/JSONLayouts/CorfuHandlerDefaultLayout.json")));

        return Layout.fromJSONString(JSONDefaultLayout);
    }
}
