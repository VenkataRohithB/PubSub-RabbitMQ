import datetime
from fastapi import FastAPI, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from qppubsub_helper import PubSubClass

app = FastAPI(title="QP Pub-Sub")

# Allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/publish/")
async def pub(topic: str, message: dict = Body(...)):
    ''' Example: {"Message":"This is DEMO message", "created_at" : "2024-04-11-17:05"} '''
    conn = PubSubClass(topic=topic)
    timestamp = datetime.datetime.now()
    message["timestamp"] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    conn.publish(msg_dict=message)
    return message


@app.get("/subscribe/")
async def sub(topic: str):
    '''Gives all messages which are published, Returns empty LIST if there are no messages posted '''
    conn = PubSubClass(topic=topic)
    try:
        messages_list = conn.receive_all(bStaticMode=True)
        if len(messages_list)>1000:
        	messages_list=messages_list[:1000]
        response = {
            "status": "success",
            "status_code": 200,
            "records": messages_list
        }
    except Exception as e:
        response = {
            "status": "failure",
            "status_code": 400,
            "record": []
        }
    return response

@app.get("/subscribe_timestamp_interval")
async def time_interval(topic: str,
                        start_time: str = Query(..., description="Start timestamp (format: 2024-05-06 15:15:00)"),
                        end_time: str = Query(..., description="End timestamp (format: 2024-05-06 15:30:00)")):
    conn = PubSubClass(topic=topic)
    return conn.get_message_in_interval(start_timestamp=start_time, end_timestamp=end_time)



if __name__ == "__main__":
    import uvicorn

    # ssl_certfile = "certs/appscrontest.qikpod.com/fullchain.pem"
    # ssl_keyfile = "certs/appscrontest.qikpod.com/privkey.pem"
    uvicorn.run(app, host="0.0.0.0", port=8000)
    # , ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile)
