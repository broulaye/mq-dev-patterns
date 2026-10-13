
import os
import ibmmq as mq

QMGR = "QM1"
QUEUE_NAME = "DEV.QUEUE.1"

# Connection details
cd = mq.CD(Version=mq.CMQXC.MQCD_VERSION_11)
cd.ChannelName = "DEV.APP.SVRCONN"
cd.ConnectionName = "localhost(1414)"
cd.ChannelType = mq.CMQXC.MQCHT_CLNTCONN
cd.TransportType = mq.CMQXC.MQXPT_TCP

csp = mq.CSP()
csp.CSPUserId = "app"
csp.CSPPassword = os.environ["MQ_APP_PASSWORD"]

qmgr = mq.QueueManager(None)
queue = None
connected = False
transaction_active = False

try:
    qmgr.connect_with_options(QMGR, cd=cd, csp=csp)
    connected = True

    queue = mq.Queue(qmgr)
    queue.open(QUEUE_NAME, mq.CMQC.MQOO_INPUT_AS_Q_DEF)

    md = mq.MD()
    gmo = mq.GMO()

    gmo.Options = (
        mq.CMQC.MQGMO_SYNCPOINT |
        mq.CMQC.MQGMO_NO_WAIT |
        mq.CMQC.MQGMO_FAIL_IF_QUIESCING
    )

    message = queue.get(None, md, gmo)
    transaction_active = True

    print("Received:", message.decode("utf-8"))

    # Simulate successful or failed processing
    result = input("Commit or backout? (c/b): ").strip().lower()

    if result == "c":
        qmgr.commit()
        transaction_active = False
        print("Transaction committed.")

    else:
        qmgr.backout()
        transaction_active = False
        print("Transaction backed out. Message can be retrieved again.")

except mq.MQMIError as error:
    if error.reason == mq.CMQC.MQRC_NO_MSG_AVAILABLE:
        print("No messages available.")
    else:
        print("MQ error:", error)

finally:
    # Don't silently commit an unfinished transaction.
    if connected and transaction_active:
        try:
            qmgr.backout()
        except mq.MQMIError as error:
            print("Backout failed:", error)

    if queue is not None:
        try:
            queue.close()
        except mq.MQMIError as error:
            print("Queue close failed:", error)

    if connected:
        try:
            qmgr.disconnect()
        except mq.MQMIError as error:
            print("Disconnect failed:", error)
