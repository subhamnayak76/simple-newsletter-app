import express from 'express';
import redis from 'redis';
import mongoose from 'mongoose';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';
import dotenv from 'dotenv';
import cors from 'cors';

dotenv.config();

const app = express();
app.use(cors());


const publisher = redis.createClient();
const subscriber = redis.createClient();


async function connectRedis() {
    try {
        await publisher.connect();
        await subscriber.connect();
        
        
        await subscriber.pSubscribe('*', (message, channel) => {
            console.log(`Received message on channel ${channel}:`, message);
            handleMessage(channel, message).catch(err => {
                console.error('Error handling message:', err);
            });
        });

        console.log('Redis clients connected successfully');
    } catch (error) {
        console.error('Redis connection error:', error);
        process.exit(1);
    }
}


publisher.on('error', (err) => console.error('Redis Publisher Error:', err));
subscriber.on('error', (err) => console.error('Redis Subscriber Error:', err));


mongoose.connect(process.env.MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
}).then(() => {
    console.log('Connected to MongoDB successfully');
}).catch((err) => {
    console.error('MongoDB connection error:', err);
});


const sesClient = new SESClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
});

// User schema
const userSchema = new mongoose.Schema({
    email: { type: String, required: true },
    topics: [{ type: String }]
});
const User = mongoose.model('User', userSchema);

app.use(express.json());


async function handleMessage(channel, message) {
    try {
        const users = await User.find({ topics: channel });
        console.log(`Found ${users.length} subscribers for channel ${channel}`);
        
        for (const user of users) {
            await sendEmailWithSES(
                user.email,
                `New update on ${channel}`,
                `<p>${message}</p>`
            );
        }
    } catch (error) {
        console.error('Error processing message:', error);
    }
}
const sendEmailWithSES = async (toEmail, subject, message) => {
    const params = {
        Destination: {
            ToAddresses: [toEmail]
        },
        Message: {
            Body: {
                Html: {
                    Data: message
                }
            },
            Subject: {
                Data: subject
            }
        },
        Source: process.env.SES_EMAIL
    };

    try {
        const command = new SendEmailCommand(params);
        const response = await sesClient.send(command);
        console.log(`Email sent to ${toEmail}:`, response);
    } catch (error) {
        console.error(`Error sending email to ${toEmail}:`, error);
    }
};

app.post('/register', async (req, res) => {
    try {
        const { email } = req.body;
        if (!email) {
            return res.status(400).send('Email is required');
        }
        const existingUser = await User.findOne({ email });
        if (existingUser) {
            return res.status(400).send('Email already exists');
        }
        const user = new User({ email, topics: [] });
        await user.save();
        return res.status(201).send('User registered successfully');
    } catch (err) {
        console.error(err);
        return res.status(500).send('Internal server error');
    }
});

app.post('/subscribe', async (req, res) => {
    try {
        const { email, topic } = req.body;
        if (!email || !topic) {
            return res.status(400).send('Email and topic are required');
        }
        const user = await User.findOne({ email });
        if (!user) {
            return res.status(400).send('User not found');
        }
        if (!user.topics.includes(topic)) {
            user.topics.push(topic);
            await user.save();
            console.log(`User ${email} subscribed to topic: ${topic}`);
        }
        return res.status(200).send(`Subscribed to ${topic}`);
    } catch (error) {
        console.error('Subscribe error:', error);
        return res.status(500).send('Internal server error');
    }
});

app.post('/publish', async (req, res) => {
    try {
        const { topic, message } = req.body;
        if (!topic || !message) {
            return res.status(400).json({ error: 'Topic and message are required' });
        }
        console.log(`Publishing message to topic ${topic}:`, message);
        const result = await publisher.publish(topic, message);
        console.log('Publish result:', result);
        res.status(200).json({ 
            message: 'Message published successfully',
            subscribersNotified: result
        });
    } catch (error) {
        console.error('Publish error:', error);
        res.status(500).json({ error: 'Failed to publish message' });
    }
});


async function startServer() {
    try {
        await connectRedis();
        app.listen(3000, () => {
            console.log('Server is running on port 3000');
        });
    } catch (error) {
        console.error('Failed to start server:', error);
        process.exit(1);
    }
}

startServer();