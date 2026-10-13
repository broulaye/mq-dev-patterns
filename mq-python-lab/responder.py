
import os
import ibmmq as mq

QMGR = "QM1"
REQUEST_QUEUE = "REQUEST.Q"

# Connection configuration
cd = mq.CD(Version=mq.CMQXC.MQCD_VERSION_11)
cd.ChannelName = "DEV.APP.SVRCONN"
cd.ConnectionName = "localhost(1414)"
cd.ChannelType = mq.CMQXC.MQCHT_CLNTCONN
cd.TransportType = mq.CMQXC.MQXPT_TCP

# Authentication
csp = mq.CSP()
csp.CSPUserId = "app"
csp.CSPPassword = os.environ["MQ_APP_PASSWORD"]

qmgr = mq.QueueManager(None)
request_queue = None
reply_queue = None
connected = False

try:
    qmgr.connect_with_options(QMGR, cd=cd, csp=csp)
    connected = True

    request_queue = mq.Queue(qmgr)
    request_queue.open(
        REQUEST_QUEUE,
        mq.CMQC.MQOO_INPUT_AS_Q_DEF
    )

    print("Responder waiting for a request...", flush=True)

    # Receive the request
    request_md = mq.MD()
    gmo = mq.GMO()
    gmo.Options = (
        mq.CMQC.MQGMO_WAIT |
        mq.CMQC.MQGMO_NO_SYNCPOINT |
        mq.CMQC.MQGMO_FAIL_IF_QUIESCING
    )
    gmo.WaitInterval = 30000

    request = request_queue.get(None, request_md, gmo)
    expression = request.decode("utf-8")
    print("Received request:", expression)

    # Accept our lab's expected expression: "5 + 3"
    left, operator, right = expression.split()
    if operator != "+":
        raise ValueError("Only addition is supported")
    result = int(left) + int(right)

    # Use the reply destination specified by the requester
    reply_name = request_md.ReplyToQ.strip()
    if not reply_name:
        raise ValueError("Request has no ReplyToQ")

    reply_queue = mq.Queue(qmgr)
    reply_queue.open(reply_name, mq.CMQC.MQOO_OUTPUT)

    # Build reply metadata
    reply_md = mq.MD()
    reply_md.Format = mq.CMQC.MQFMT_STRING
    reply_md.MsgType = mq.CMQC.MQMT_REPLY

    # Link this reply to the original request
    reply_md.CorrelId = request_md.MsgId

    pmo = mq.PMO()
    pmo.Options = mq.CMQC.MQPMO_NO_SYNCPOINT

    reply_queue.put(str(result), reply_md, pmo)

    print("Sent reply:", result)
    print("Reply CorrelId:", reply_md.CorrelId.hex())

except mq.MQMIError as error:
    print("MQ error:", error)

except (ValueError, UnicodeDecodeError) as error:
    print("Invalid request:", error)

finally:
    if reply_queue is not None:
        reply_queue.close()
    if request_queue is not None:
        request_queue.close()
    if connected:
        qmgr.disconnect()
