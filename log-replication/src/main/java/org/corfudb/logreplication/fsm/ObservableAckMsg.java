package org.corfudb.logreplication.fsm;
import lombok.Data;
import org.corfudb.logreplication.message.DataMessage;

import java.util.Observable;

/**
 * This class represents an observable value which reflects the number of
 * received messages and the last received DataMessage, i.e.,
 * an object that the application will observe
 * to receive notifications on change.
 */
@Data
public class ObservableAckMsg extends Observable
{
    int msgCnt;
    DataMessage dataMessage;

    public ObservableAckMsg() {
        this.msgCnt = 0;
        this.dataMessage = null;
    }

    public void setValue(DataMessage dataMessage)
    {
        if (dataMessage != null) {
            this.dataMessage = dataMessage;
            msgCnt++;
        }
        setChanged();
        notifyObservers(dataMessage);
    }

    public ObservableAckMsg getValue()
    {
        return this;
    }
}