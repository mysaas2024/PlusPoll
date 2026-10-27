import amqp from 'amqplib'

const RABBIT= process.env.RABBITMQ_URL

if(!RABBIT){
    throw new Error("RabbitMQL:Enironments Not Ready");
}
const EXCHANGE ='email.reset'
const QUEUE='email.rest'
const RountingKey='rest.email.key'

const StartWoker= async()=>{
    try{
        const connection= await amqp.connect(RABBIT);
        const channel= await connection.createChannel()

        await channel.assertExchange(EXCHANGE, {
            durable:true
        });

        await channel.assertQueue(QUEUE,{
            durable:true,
            exclusive:false
        })

        await channel.assertQueue(QUEUE,EXCHANGE, RountingKey);
        channel.prefetch(1);

        console.log("Worker Started:Password");
        console.log("Queue",QUEUE);

        channel.consume(QUEUE, async (message)=>{
            if(!message){
                console.log('No Message')
            }
            try{
              const job= JSON.parse(message.content.toString())
              console.log(`Processing JOB:${job.id}`)

                const {to , subject , body }= job.data

                console.log({to ,subject, body});

                console.log("email Send");

                channel.ack(message);

               
            }catch(error){
                console.log("Intervnal Password Reset Erorr");
                console.log(error);
            }



        })







    

    }catch(error){
        console.log("Reset Email Worker Error");
        console.log(error.mesage);
    }
}