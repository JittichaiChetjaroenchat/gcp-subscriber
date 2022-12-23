const { GoogleAuth } = require('google-auth-library');
const { PubSub } = require('@google-cloud/pubsub');
const grpc = require('@grpc/grpc-js');
const EventEmitter = require('events');

const { createTopic } = require('./../publisher');

require('dotenv').config();

async function getSubscription(client, subscriptionName) {
    const [subscriptions] = await client.getSubscriptions();
    const subscription = subscriptions.find((subscription) => subscription.name === `projects/${client.projectId}/subscriptions/${subscriptionName}`);

    return subscription;
}

async function createSubscription(client, topic, subscriptionName, subscribionOptions) {
    // Check exists
    const exists = await getSubscription(client, subscriptionName);
    if (exists) {
        return exists;
    }

    // Create new
    const [ created ] = await topic.createSubscription(subscriptionName, subscriberOptions);

    return created;
}

function createClient(projectId, clientEmail, privateKey) {
    try {
        // Create client
        const credentials = { client_email: clientEmail, private_key: privateKey };
        const auth = new GoogleAuth({ credentials });
        const client = new PubSub({ auth, projectId, grpc});

        return client;
    } catch(error) {
        console.error(error);
        throw new Error('Error occurred when authen.');
    }
}

async function subscribe(subscription) {
    try {
        // Listening and pass events out
        const eventEmitter = new EventEmitter();
        subscription.on('message', message => eventEmitter.emit('message', message));
        subscription.on('close', () => eventEmitter.emit('close'));
        subscription.on('debug', message => eventEmitter.emit('debug', message));
        subscription.on('error', error => eventEmitter.emit('error', error));

        return eventEmitter;
    } catch (error) {
        console.error(error);
        throw new Error('Error occurred when subscribe.');
    }
}

module.exports = { createClient, createTopic, createSubscription, subscribe };