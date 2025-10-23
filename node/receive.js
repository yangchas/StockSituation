const rabbit = require("rabbitmq-stream-js-client")

async function main() {
	    const streamName = "stream8"

	    console.log("Connecting...");
	    const client = await rabbit.connect({
		            hostname: "localhost",
		            port: 5552,
		            username: "admin",
		            password: "admin",
		            vhost: "/",
		        })

	    console.log("Making sure the stream exists...");
	    const streamSizeRetention = 100 * 1024 * 1024
	    await client.createStream({ stream: streamName, arguments: { "max-length-bytes": streamSizeRetention } });

	    console.log("Declaring the consumer with offset...");
	    await client.declareConsumer({ stream: streamName, offset: rabbit.Offset.last() }, (message) => {
		            console.log(`Received message ${message.content.toString()}`)
		        })

}

main()
    .then(async () => {
	            await new Promise(function () { })
	        })
    .catch((res) => {
	            console.log("Error while receiving message!", res)
	            process.exit(-1)
	        })
