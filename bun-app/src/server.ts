import { Elysia } from 'elysia'
import { MongoClient, ServerApiVersion } from 'mongodb'
import { Product } from './dtos'

const uri =
  process.env.MONGO_URL ??
  'mongodb://mongodb1:27017,mongodb2:27018,mongodb3:27019/service_product?replicaSet=my-replica-set'
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
})

async function startMongo() {
  try {
    // Connect the client to the server (optional starting in v4.7)
    const db = await client.connect()
    // Send a ping to confirm a successful connection
    await client.db('admin').command({ ping: 1 })
    console.log('Pinged your deployment. You successfully connected to MongoDB!')
    return db
  } finally {
    // Ensures that the client will close when you finish/error
    // await client.close()
  }
}

export default class Server {
  static async start() {
    const client = await startMongo()
    const app = new Elysia()
    const db = client.db('products')

    app.get('/', () => 'Hello Elysia')

    app.get('/products', async ({ request, query }) => {
      const start = Date.now()
      const collection = db.collection<Product>('products')

      const projection: Record<string, number> = { _id: 0 }

      const { fields } = query
      if (fields) {
        const fieldsArray = fields.split(',')
        fieldsArray.forEach((field) => {
          projection[field] = 1
        })
      }
      const products = await collection.find({ deleteDate: null }, { projection }).toArray()
      const response: Product[] = []

      for (const product of products) {
        response.push({
          ...product,
          href: `${request.url.split('?')[0]}/${product.id}`,
        })
      }

      const durationInMs = Date.now() - start
      return {
        time: (durationInMs / 1000).toFixed(2) + ' ms',
        products: response.slice(0, 100),
        count: products.length,
      }
    })

    app.get('/products/:id', async ({ request, params }) => {
      const start = Date.now()
      const collection = db.collection<Product>('products')

      const { id } = params

      const product = await collection.findOne({ deleteDate: null, id })
      if (product?.id) {
        const url = request.url.split('?')[0]
        product.href = `${url}/${product.id}`
      }

      const durationInMs = Date.now() - start
      return {
        time: (durationInMs / 1000).toFixed(2) + ' ms',
        product: product,
      }
    })

    app.listen(3000)

    console.log(`🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}`)
  }
}
