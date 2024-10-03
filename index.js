import express from 'express';
import redis from 'redis'
import mongoose from 'mongoose'
import sgMail from '@sendgrid/mail';
import {Resend}from 'resend';
import dotenv from 'dotenv'
const app = express()

const publisher = redis.createClient()
const subscriber = redis.createClient()
// send grid api key
dotenv.config()
const resend = new Resend(process.env.RESEND_API_KEY);


mongoose.connect(process.env.MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

const userSchema = new mongoose.Schema({
    email: { type: String, required: true },
    topics: [{ type: String }] 
});

const User = mongoose.model('User', userSchema);
app.use(express.json())

app.post('/register',async (req,res) =>{
    try {
    const email = req.body.email
    if(!email){
        return res.status(400).send('Email is required')
    }
    const exitinguser = await User.findOne({email})
    if(exitinguser){
        return res.status(400).send('Email already exists')
    }
    const user = new User({
        email,
        topics: []
    })
    await user.save()

    return res.status(201).send('User registered successfully')
    }
    catch(err){
        console.log(err)
        return res.status(500).send('Internal server error')
    }

}) 
app.post('/subscribe',async (req,res)=>{
    const email = req.body.email
    const topic = req.body.topic
    if(!email || !topic){
        return res.status(400).send('Email and topic are required')
    }
    const user = await User.findOne({email})
    if(!user){
        return res.status(400).send('User not found')
    }
    if(!user.topics.includes(topic)){
        user.topics.push(topic)
    }
    await user.save()
    return res.status(200).send(`Subscribed to ${topic}`)
})

app.post('/publish', (req, res) => {
    const { topic, message } = req.body;

    // Publish message to Redis Pub/Sub
    publisher.publish(topic, message, () => {
        res.status(200).send('Message published');
    });
});

app.post('/subscribe-topic', (req, res) => {
    const { topic } = req.body;
    subscriber.subscribe(topic, () => {
        res.status(200).send(`Subscribed to Redis topic: ${topic}`);
    });
});


subscriber.on('message', async (channel, message) => {
    console.log(`Received message from topic: ${channel}`);
    
    const users = await User.find({ topics: channel });
    
    
    for (const user of users) {
        try {
            await resend.emails.send({
                from: 'no-reply@resend.dev', // Use your verified domain or Resend's shared domain
                to: user.email,
                subject: `New update on ${channel}`,
                html: `<p>${message}</p>`, // You can use HTML formatting here
            });
            console.log(`Email sent to ${user.email}`);
        } catch (error) {
            console.error(`Error sending email to ${user.email}:`, error);
        }
    }
});
    
    
app.listen(3000, () => {
    console.log('Server is running on port 3000')
})