import express, { Application, Request, Response } from 'express'
import { getDatabase } from './db'
import { Product } from './dtos'

class Server {
  static async start() {
    const app: Application = express(),
      port = process.env.PORT ?? 3000

    const db = await getDatabase()
    const col = db.collection<Product>('products')

    app.use(express.urlencoded({ extended: true }))
    app.use(express.json({ limit: '50mb' }))

    app.get('/healthz', (req: Request, res: Response) => {
      res.sendStatus(200)
    })

    app.get('/products', async (req: Request, res: Response) => {
      const start = Date.now()
      try {
        const products = await col.find({ deleteDate: null }, { projection: { _id: 0, id: 1, name: 1 } }).toArray()
        const response: Product[] = []
        for (const product of products) {
          response.push({
            id: product.id,
            name: product.name,
            href: `${req.protocol}://${req.get('host')}/products/${product.id}`,
          })
        }

        const durationInMs = Date.now() - start
        res.json({
          products: response.slice(0, 100),
          count: products.length,
          time: (durationInMs / 1000).toFixed(2) + ' ms',
        })
      } catch (error) {}
    })

    const server = app.listen(port, () => {
      console.log(`Server is running on port ${port}`)
    })

    process.on('SIGTERM', () => {
      console.log('SIGTERM signal received: closing HTTP server')
      server.close(() => process.exit(0))
    })
    process.on('SIGINT', () => {
      console.log('SIGINT signal received: closing HTTP server')
      server.close(() => process.exit(0))
    })
  }
}

export default Server
