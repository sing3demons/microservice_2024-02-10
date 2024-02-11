import dotenv from 'dotenv'
import KafkaServer from './server'
import { faker } from '@faker-js/faker'
import { customAlphabet } from 'nanoid'
import { Price, Product, SupportingLanguage } from './dtos'

function generateId() {
  const nanoid = customAlphabet('0123456789AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz', 11)
  return nanoid()
}

dotenv.config()

async function createProducts() {
  const products = []
  const productLanguage = []

  for (let i = 0; i < 5000; i++) {
    const productLanguageEn: SupportingLanguage = {
      id: generateId(),
      name: faker.commerce.productName(),
      description: faker.commerce.productDescription(),
      languageCode: 'en',
      status: 'active',
      createdAt: new Date(),
      updatedAt: new Date(),
      attachment: [
        {
          id: generateId(),
          name: faker.commerce.productName(),
          url: faker.image.avatar(),
          type: 'image',
          createdAt: new Date(),
          updatedAt: new Date(),
          redirectUrl: faker.internet.url(),
        },
      ],
    }
    const productLanguageTh: SupportingLanguage = {
      id: generateId(),
      name: faker.commerce.productName(),
      description: faker.commerce.productDescription(),
      languageCode: 'th',
      status: 'active',
      createdAt: new Date(),
      updatedAt: new Date(),
      attachment: [
        {
          id: generateId(),
          name: faker.commerce.productName(),
          url: faker.image.avatar(),
          type: 'image',
          createdAt: new Date(),
          updatedAt: new Date(),
          redirectUrl: faker.internet.url(),
        },
      ],
    }

    productLanguage.push(productLanguageEn, productLanguageTh)

    const price: Price = {
      id: generateId(),
      unitOfMeasure: {
        unit: 'THB',
        amount: Math.random() * 1000,
        currency: '฿',
      },
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    const product: Product = {
      id: generateId(),
      name: faker.commerce.productName(),
      SupportingLanguage: [
        {
          id: productLanguageEn.id,
          name: productLanguageEn.name,
          languageCode: productLanguageEn.languageCode,
        },
        {
          id: productLanguageTh.id,
          name: productLanguageTh.name,
          languageCode: productLanguageTh.languageCode,
        },
      ],
      status: 'active',
      createdAt: new Date(),
      updatedAt: new Date(),
      price: [price],
    }
    products.push(product)
  }

  console.log('products :: ', products.length)
  console.log('productLanguage :: ', productLanguage.length)


  const result = await Promise.allSettled([
    KafkaServer.multipleProducer('create.products', products),
    KafkaServer.multipleProducer('create.productsLanguage', productLanguage)
  ])

  let success = 0

  result.forEach((res) => {
    if (res.status === 'rejected') {
      console.log(res.reason)
    } else {
      success++
    }
  })

  console.log('success :: ', success)
}

// main()
createProducts()
createProducts()

async function createProduct() {
  const products = []
  const productLanguage = []

  const producer = new KafkaServer()
  await producer.connect()

  for (let i = 0; i < 10000; i++) {
    const productLanguageEn: SupportingLanguage = {
      id: generateId(),
      name: faker.commerce.productName(),
      description: faker.commerce.productDescription(),
      languageCode: 'en',
      status: 'active',
      createdAt: new Date(),
      updatedAt: new Date(),
      attachment: [
        {
          id: generateId(),
          name: faker.commerce.productName(),
          url: faker.image.avatar(),
          type: 'image',
          createdAt: new Date(),
          updatedAt: new Date(),
          redirectUrl: faker.internet.url(),
        },
      ],
    }
    const productLanguageTh: SupportingLanguage = {
      id: generateId(),
      name: faker.commerce.productName(),
      description: faker.commerce.productDescription(),
      languageCode: 'th',
      status: 'active',
      createdAt: new Date(),
      updatedAt: new Date(),
      attachment: [
        {
          id: generateId(),
          name: faker.commerce.productName(),
          url: faker.image.avatar(),
          type: 'image',
          createdAt: new Date(),
          updatedAt: new Date(),
          redirectUrl: faker.internet.url(),
        },
      ],
    }

    productLanguage.push(productLanguageEn, productLanguageTh)

    const price: Price = {
      id: generateId(),
      unitOfMeasure: {
        unit: 'THB',
        amount: Math.random() * 1000,
        currency: '฿',
      },
      createdAt: new Date(),
      updatedAt: new Date(),
    }

    const product: Product = {
      id: generateId(),
      name: faker.commerce.productName(),
      SupportingLanguage: [
        {
          id: productLanguageEn.id,
          name: productLanguageEn.name,
          languageCode: productLanguageEn.languageCode,
        },
        {
          id: productLanguageTh.id,
          name: productLanguageTh.name,
          languageCode: productLanguageTh.languageCode,
        },
      ],
      status: 'active',
      createdAt: new Date(),
      updatedAt: new Date(),
      price: [price],
    }
    products.push(product)
  }

  const start = performance.now()
  console.log('Start...')
  const createProducts = []
  for (const product of products) {
    createProducts.push(KafkaServer.producer('create.products', product))
    // createProducts.push(producer.producerOne('create.products', product))
  }

  //   const createProductLanguage = []
  for (const product of productLanguage) {
    // createProductLanguage.push(producer.producerOne('create.productsLanguage', product))
    createProducts.push(producer.producerOne('create.productsLanguage', product))
  }

  const resultProduct = await Promise.all(createProducts)
  console.log(resultProduct.length)
  //   const resultProductLanguage = await Promise.all(createProductLanguage)
  //   console.log(resultProductLanguage.length)

  console.log('All messages sent :: ', (performance.now() - start).toFixed(3), 'ms')
  await producer.destroy()
}

// createProduct()
