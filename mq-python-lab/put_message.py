import os
import ibmmq as mq


QMGR = "QM1"
CHANNEL = "DEV.APP.SVRCONN"
CONNECTION = "localhost(1414)"
QUEUE_NAME = "DEV.QUEUE.1"

USER = "app"
PASSWORD = os.environ["MQ_APP_PASSWORD"]


# Describe how the MQ client should connect to QM1.
cd = mq.CD(Version=mq.CMQXC.MQCD_VERSION_11)

cd.ChannelName = CHANNEL
cd.ConnectionName = CONNECTION
cd.ChannelType = mq.CMQXC.MQCHT_CLNTCONN
cd.TransportType = mq.CMQXC.MQXPT_TCP


# Supply application credentials.
csp = mq.CSP()

csp.CSPUserId = USER
csp.CSPPassword = PASSWORD


# Connect to the queue manager.
qmgr = mq.QueueManager(None)

qmgr.connect_with_options(
    QMGR,
    csp=csp,
    cd=cd
)


# Open DEV.QUEUE.1 for output.
queue = mq.Queue(qmgr)

od = mq.OD()
od.ObjectName = QUEUE_NAME

queue.open(
    od,
    mq.CMQC.MQOO_OUTPUT
)


# Describe and send our message.
md = mq.MD()
md.Format = mq.CMQC.MQFMT_STRING
md.Persistence = mq.CMQC.MQPER_PERSISTENT

pmo = mq.PMO()
pmo.Options = (
    mq.CMQC.MQPMO_NO_SYNCPOINT |
    mq.CMQC.MQPMO_NEW_MSG_ID
)

message = "Learning MQ message IDs with Python"

queue.put(message, md, pmo)

print("Message successfully sent.")
print("Message ID:", md.MsgId.hex())
print("Correlation ID:", md.CorrelId.hex())


# Clean up.
queue.close()
qmgr.disconnect()