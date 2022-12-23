const subscriber = require('./subscriber');

require('dotenv').config();

const projectId = process.env.GOOGLE_PUBSUB_PROJECT_ID;
const clientEmail = process.env.GOOGLE_PUBSUB_CLIENT_EMAIL;
const privateKey = process.env.GOOGLE_PUBSUB_CLIENT_PRIVATE_KEY.replace(/\\n/g, '\n');
const topicName = process.env.GOOGLE_PUBSUB_TOPIC_NAME;
const client = subscriber.createClient(projectId, clientEmail, privateKey);

async function boostraper() {
    // Get/Create topic and subscription
    const topic = await subscriber.createTopic(client, topicName);
    const subscriptionName = process.env.GOOGLE_PUBSUB_SUBSCRIPTION_NAME;
    const subscriberOptions = {
        enableMessageOrdering: Boolean(process.env.GOOGLE_PUBSUB_SUBSCRIPTION_ENABLE_MESSAGE_ORDERING),
        allowExcessMessages: Boolean(process.env.GOOGLE_PUBSUB_SUBSCRIPTION_ALLOW_EXCESS_MESSAGES),
        ackDeadlineSeconds: Number(process.env.GOOGLE_PUBSUB_SUBSCRIPTION_ACK_DEADLINE_SECONDS),
    };
    const subscription = await subscriber.createSubscription(client, topic, subscriptionName, subscriberOptions);

    // Subscribe message
    const subscribe1 = await subscriber.subscribe(subscription);
    subscribe1.on('message', message => {
        const data = JSON.parse(message.data);

        // Ack message
        console.log(`[${message.publishTime}] - ACK FOR MESSGE ID: ${data.id}`);
        message.ack();
    });
    // subscribe1.on('close', () => {
    //     console.error('Received close');
    // });
    // subscribe1.on('debug', error => {
    //     console.error('Received debug:', error);
    // });
    // subscribe1.on('error', error => {
    //     console.error('Received error:', error);
    // });
}

boostraper()
    .then(() => {
        console.log('Worker is listening...');
    })
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });