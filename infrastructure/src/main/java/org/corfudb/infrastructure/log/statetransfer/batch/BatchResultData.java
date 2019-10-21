package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A successful result of a transfer,
 * which contains the number of the total addresses transferred.
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class BatchResultData {
    private final long addressesTransferred;
}