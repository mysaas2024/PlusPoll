import amqp from 'amqplib'

const RABBITMQ_URL=process.env.RABBITMQ_URL;
if(!RABBITMQ_URL){
    throw new Error("Rabbit MQ URL not inilized")
}

const EXHANGE='email.exchange'
const QUEUE='welcome.mail.queue'
const RoutingKey='welcome.poll.key'

const StartWork= async()=>{
    try{
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel= await connection.createChannel();

        await channel.assertExchange(EXHANGE, "direct" ,{
            durable:true
        })

        await channel.assertQueue(QUEUE ,{
            durable:true,
            exclusive:false
        })

        await channel.bindQueue(QUEUE,EXHANGE, RoutingKey);
        channel.prefetch(1)
        console.log("Welcome Email Started")
        console.log(`Listen to Queue ${QUEUE}`);

        channel.consume(
            QUEUE, async (message)=>{
                if(!message){
                    console.log("No message");
                }

                try{
                    const job= JSON.parse(message.content.toString())
                    console.log(`Processing JOB:${job.id}`)

                const {to , subject , body }= job.data

                console.log({to ,subject, body});

                console.log("email Send");

                channel.ack(message);

                }catch(error){
                    console.log("Rabbit MQ worker Failed")
                    console.log(error.message)
                }

            }
        )


    }catch(error){
        console.log('Welcome Eamil Worker Error');
        console.log(error.message)
    }
}