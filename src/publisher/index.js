const { GoogleAuth } = require('google-auth-library');
const { PubSub } = require('@google-cloud/pubsub');
const grpc = require('@grpc/grpc-js');

require('dotenv').config();

async function getTopic(client, topicName) {
    const projectId = process.env.GOOGLE_PUBSUB_PROJECT_ID;
    const [topics] = await client.getTopics();
    const topic = topics.find((topic) => topic.name === `projects/${projectId}/topics/${topicName}`);

    return topic;
}

async function createTopic(client, topicName) {
    // Check exists
    const exists = await getTopic(client, topicName);
    if (exists) {
        return exists;
    }

    // Create new
    const [ created ] = await client.createTopic(topicName);

    return created;
}

module.exports = { createTopic };