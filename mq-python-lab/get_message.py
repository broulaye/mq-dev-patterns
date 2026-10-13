
import os
import ibmmq as mq

QMGR = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONNECTION = "localhost(1414)"
QUEUE_NAME = "DEV.QUEUE.1"

# Connection configuration
cd = mq.CD(Version=mq.CMQXC.MQCD_VERSION_11)
cd.ChannelName = CHANNEL
cd.ConnectionName = CONNECTION
cd.ChannelType = mq.CMQXC.MQCHT_CLNTCONN
cd.TransportType = mq.CMQXC.MQXPT_TCP

# Authentication
csp = mq.CSP()
csp.CSPUserId = "app"
csp.CSPPassword = os.environ["MQ_APP_PASSWORD"]

qmgr = mq.QueueManager(None)
queue = None

try:
    # Connect to queue manager
    qmgr.connect_with_options(QMGR, cd=cd, csp=csp)

    # Open queue for reading
    queue = mq.Queue(qmgr)
    queue.open(QUEUE_NAME, mq.CMQC.MQOO_INPUT_AS_Q_DEF)

    # Retrieve one message

    md = mq.MD()
    gmo = mq.GMO()

    gmo.Options = (
        mq.CMQC.MQGMO_WAIT |
        mq.CMQC.MQGMO_NO_SYNCPOINT
    )

    # IBM MQ measures this timeout in milliseconds.
    gmo.WaitInterval = 10000  # 10 seconds

    # md = mq.MD()
    # gmo = mq.GMO()
    # gmo.Options = mq.CMQC.MQGMO_NO_WAIT | mq.CMQC.MQGMO_NO_SYNCPOINT

    message = queue.get(None, md, gmo)

    print("Received:", message.decode("utf-8"))
    print("Message ID:", md.MsgId.hex())
    print("Correlation ID:", md.CorrelId.hex())

except mq.MQMIError as error:
    if error.reason == mq.CMQC.MQRC_NO_MSG_AVAILABLE:
        print("No messages available on the queue.")
    else:
        print("Unexpected MQ error:", error)
        raise

finally:
    if queue is not None:
        queue.close()
    if qmgr.is_connected:
        qmgr.disconnect()
