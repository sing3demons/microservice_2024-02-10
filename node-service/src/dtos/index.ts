export interface Price {
  id?: string
  name?: string
  tax?: {
    type?: string
    value?: number
  }
  popRelationship?: IPopRelationship[]
  unitOfMeasure?: UnitOfMeasure
  SupportingLanguage?: SupportingLanguage[]
  status?: string
  createdAt?: Date
  updatedAt?: Date
}

interface IPopRelationship {
  id?: string
  name?: string
}

interface UnitOfMeasure {
  unit?: string
  amount?: number
  currency?: string
}

export interface Category {
  id?: string
  name?: string
  description?: string
  SupportingLanguage?: SupportingLanguage[]
  status?: string
  createdAt?: Date
  updatedAt?: Date
}

export interface SupportingLanguage {
  id?: string
  name?: string
  description?: string
  languageCode?: string
  unitOfMeasure?: UnitOfMeasure
  attachment?: Attachment[]
  status?: string
  createdAt?: Date
  updatedAt?: Date
}

interface Attachment {
  id?: string
  name?: string
  url?: string
  type?: string
  status?: string
  createdAt?: Date
  updatedAt?: Date
  display?: {
    type?: string
    value?: string
  }
  redirectUrl?: string
}

export interface Product {
  id?: string
  name?: string
  price?: Price[]
  category?: Category[]
  description?: string
  stock?: number
  status?: string
  createdAt?: Date
  updatedAt?: Date
  SupportingLanguage?: SupportingLanguage[]
}
