
import os
import ibmmq as mq

QMGR = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONNECTION = "localhost(1414)"
QUEUE_NAME = "DEV.QUEUE.1"

# Client connection configuration
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
connected = False

try:
    qmgr.connect_with_options(QMGR, cd=cd, csp=csp)
    connected = True

    queue = mq.Queue(qmgr)
    queue.open(
        QUEUE_NAME,
        mq.CMQC.MQOO_INPUT_AS_Q_DEF
    )

    print("Consumer running. Press Ctrl+C to stop.", flush=True)

    while True:
        # Fresh descriptor for each GET
        md = mq.MD()
        gmo = mq.GMO()

        gmo.Options = (
            mq.CMQC.MQGMO_WAIT |
            mq.CMQC.MQGMO_NO_SYNCPOINT |
            mq.CMQC.MQGMO_FAIL_IF_QUIESCING
        )

        gmo.WaitInterval = 5000  # 5 seconds

        try:
            message = queue.get(None, md, gmo)
            print("Received:", message.decode("utf-8"))

        except mq.MQMIError as error:
            if error.reason == mq.CMQC.MQRC_NO_MSG_AVAILABLE:
                # Normal timeout, not a fatal error
                continue
            elif error.reason == mq.CMQC.MQRC_Q_MGR_QUIESCING:
                print("Queue manager is shutting down.")
                break
            else:
                raise

except KeyboardInterrupt:
    print("\nShutdown requested.")

except mq.MQMIError as error:
    print("Unexpected MQ error:", error)

finally:
    if queue is not None:
        try:
            queue.close()
        except mq.MQMIError as error:
            print("Error closing queue:", error)

    if connected:
        try:
            qmgr.disconnect()
        except mq.MQMIError as error:
            print("Error disconnecting:", error)

    print("Consumer stopped.")
