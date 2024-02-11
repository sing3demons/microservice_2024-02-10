import { startMongo } from './db'
import Server from './server'
import dotenv from 'dotenv'
dotenv.config()

// startMongo().catch(console.dir)
Server.start()
