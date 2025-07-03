require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const AWS = require('aws-sdk');
const multer = require('multer');
const { logInfo, logError } = require('./logger');
const swaggerDocs = require('./swagger');
const mysql = require('mysql2/promise');

const app = express();
app.use(express.json());

/**
 * @swagger
 * tags:
 *   - name: CRUD MongoDb
 *     description: Operações de CRUD para usuários no MongoDb.
 *   - name: Buckets
 *     description: Operações com Buckets S3.
 *   - name: Produtos
 *     description: CRUD para produtos no MySQL
 */

//#region MongoDB
mongoose.connect(process.env.MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
}).then(() => logInfo('MongoDB conectado', null))
  .catch(err => logError('Erro ao conectar MongoDB: ' + err, null, err));

const UserSchema = new mongoose.Schema({
  nome: String,
  email: String,
});
const User = mongoose.model('Usuario', UserSchema);

/**
 * @swagger
 * /usuarios:
 *   post:
 *     tags:
 *       - CRUD MongoDb
 *     summary: Criar um novo usuário
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               nome:
 *                 type: string
 *               email:
 *                 type: string
 *             required:
 *               - nome
 *               - email
 *     responses:
 *       201:
 *         description: Usuário criado com sucesso.
 *       500:
 *         description: Erro interno
 */
app.post('/usuarios', async (req, res) => {
  try {
    const user = new User(req.body);
    await user.save();
    logInfo('Usuário criado', req, user);
    res.status(201).send(user);
  } catch (error) {
    logError('Erro ao criar usuário', req, error);
    res.status(500).send('Erro interno');
  }
});

/**
 * @swagger
 * /usuarios:
 *   get:
 *     tags:
 *       - CRUD MongoDb
 *     summary: Listar todos os usuários
 *     responses:
 *       200:
 *         description: Lista de usuários
 */
app.get('/usuarios', async (req, res) => {
  try {
    const users = await User.find();
    logInfo('Usuários listados', req, users);
    res.send(users);
  } catch (error) {
    logError('Erro ao buscar usuários', req, error);
    res.status(500).send('Erro interno');
  }
});

/**
 * @swagger
 * /usuarios/{id}:
 *   get:
 *     tags:
 *       - CRUD MongoDb
 *     summary: Obter um usuário por ID
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Usuário encontrado
 *       404:
 *         description: Usuário não encontrado
 */
app.get('/usuarios/:id', async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) return res.status(404).send('Usuário não encontrado');
    logInfo('Usuário encontrado', req, user);
    res.send(user);
  } catch (error) {
    logError('Erro ao buscar usuário', req, error);
    res.status(500).send('Erro interno');
  }
});

/**
 * @swagger
 * /usuarios/{id}:
 *   put:
 *     tags:
 *       - CRUD MongoDb
 *     summary: Atualizar um usuário
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               nome:
 *                 type: string
 *               email:
 *                 type: string
 *     responses:
 *       200:
 *         description: Usuário atualizado
 *       404:
 *         description: Usuário não encontrado
 */
app.put('/usuarios/:id', async (req, res) => {
  try {
    const user = await User.findByIdAndUpdate(req.params.id, req.body, { new: true });
    if (!user) return res.status(404).send('Usuário não encontrado');
    logInfo('Usuário atualizado', req, user);
    res.send(user);
  } catch (error) {
    logError('Erro ao atualizar usuário', req, error);
    res.status(500).send('Erro interno');
  }
});

/**
 * @swagger
 * /usuarios/{id}:
 *   delete:
 *     tags:
 *       - CRUD MongoDb
 *     summary: Deletar um usuário
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Usuário deletado
 *       404:
 *         description: Usuário não encontrado
 */
app.delete('/usuarios/:id', async (req, res) => {
  try {
    const result = await User.deleteOne({ _id: req.params.id });
    if (result.deletedCount === 0) return res.status(404).send('Usuário não encontrado');
    logInfo('Usuário removido', req);
    res.send({ message: 'Usuário removido com sucesso' });
  } catch (error) {
    logError('Erro ao remover usuário', req, error);
    res.status(500).send('Erro interno');
  }
});
//#endregion

//#region MySQL (CRUD Produtos)

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  port: process.env.MYSQL_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

/**
 * @swagger
 * /produtos:
 *   post:
 *     tags:
 *       - Produtos
 *     summary: Criar um novo produto
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               nome:
 *                 type: string
 *               preco:
 *                 type: number
 *             required:
 *               - nome
 *               - preco
 *     responses:
 *       201:
 *         description: Produto criado com sucesso.
 *       500:
 *         description: Erro interno
 */
app.post('/produtos', async (req, res) => {
  try {
    const { nome, preco } = req.body;
    const [result] = await pool.query('INSERT INTO produtos (nome, preco) VALUES (?, ?)', [nome, preco]);
    res.status(201).json({ id: result.insertId, nome, preco });
  } catch (error) {
    logError('Erro ao criar produto', req, error);
    res.status(500).send('Erro ao criar produto');
  }
});

/**
 * @swagger
 * /produtos:
 *   get:
 *     tags:
 *       - Produtos
 *     summary: Listar todos os produtos
 *     responses:
 *       200:
 *         description: Lista de produtos
 */
app.get('/produtos', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM produtos');
    res.json(rows);
  } catch (error) {
    logError('Erro ao listar produtos', req, error);
    res.status(500).send('Erro ao listar produtos');
  }
});

/**
 * @swagger
 * /produtos/{id}:
 *   put:
 *     tags:
 *       - Produtos
 *     summary: Atualizar um produto
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               nome:
 *                 type: string
 *               preco:
 *                 type: number
 *     responses:
 *       200:
 *         description: Produto atualizado
 *       404:
 *         description: Produto não encontrado
 */
app.put('/produtos/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { nome, preco } = req.body;
    const [result] = await pool.query('UPDATE produtos SET nome = ?, preco = ? WHERE id = ?', [nome, preco, id]);
    if (result.affectedRows === 0) return res.status(404).send('Produto não encontrado');
    res.send({ id, nome, preco });
  } catch (error) {
    logError('Erro ao atualizar produto', req, error);
    res.status(500).send('Erro ao atualizar produto');
  }
});

/**
 * @swagger
 * /produtos/{id}:
 *   delete:
 *     tags:
 *       - Produtos
 *     summary: Deletar um produto
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *     responses:
 *       200:
 *         description: Produto deletado
 *       404:
 *         description: Produto não encontrado
 */
app.delete('/produtos/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const [result] = await pool.query('DELETE FROM produtos WHERE id = ?', [id]);
    if (result.affectedRows === 0) return res.status(404).send('Produto não encontrado');
    res.send({ message: 'Produto removido com sucesso' });
  } catch (error) {
    logError('Erro ao deletar produto', req, error);
    res.status(500).send('Erro ao deletar produto');
  }
});

//#endregion

//#region S3
AWS.config.update({
  accessKeyId: process.env.ACCESS_KEY_ID,
  secretAccessKey: process.env.SECRET_ACCESS_KEY,
  region: process.env.REGION,
  sessionToken: process.env.SESSION_TOKEN,
});
const s3 = new AWS.S3();
const upload = multer({ storage: multer.memoryStorage() });

/**
 * @swagger
 * /buckets:
 *   get:
 *     tags:
 *       - Buckets
 *     summary: Listar buckets S3
 */
app.get('/buckets', async (req, res) => {
  try {
    const data = await s3.listBuckets().promise();
    logInfo('Buckets listados', req, data.Buckets);
    res.json(data.Buckets);
  } catch (error) {
    logError('Erro ao listar buckets', req, error);
    res.status(500).send('Erro ao listar buckets');
  }
});

/**
 * @swagger
 * /buckets/{bucketName}/upload:
 *   post:
 *     tags:
 *       - Buckets
 *     summary: Upload de arquivo para bucket
 *     parameters:
 *       - in: path
 *         name: bucketName
 *         required: true
 *         schema:
 *           type: string
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               file:
 *                 type: string
 *                 format: binary
 *     responses:
 *       200:
 *         description: Arquivo enviado com sucesso
 */
app.post('/buckets/:bucketName/upload', upload.single('file'), async (req, res) => {
  const { bucketName } = req.params;
  const file = req.file;

  if (!file) {
    logError('Arquivo não enviado', req);
    return res.status(400).send('Nenhum arquivo enviado.');
  }

  const params = {
    Bucket: bucketName,
    Key: file.originalname,
    Body: file.buffer,
  };

  try {
    const result = await s3.upload(params).promise();
    logInfo('Upload efetuado', req, result);
    res.status(200).json({ message: 'Arquivo enviado com sucesso!', result });
  } catch (error) {
    logError('Erro ao efetuar upload', req, error);
    res.status(500).json({ error: 'Erro ao enviar arquivo', details: error });
  }
});

/**
 * @swagger
 * /buckets/{bucketName}/file/{fileName}:
 *   delete:
 *     tags:
 *       - Buckets
 *     summary: Deletar arquivo de bucket
 *     parameters:
 *       - in: path
 *         name: bucketName
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: fileName
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Arquivo deletado com sucesso
 */
app.delete('/buckets/:bucketName/file/:fileName', async (req, res) => {
  const { bucketName, fileName } = req.params;

  const params = {
    Bucket: bucketName,
    Key: fileName,
  };

  try {
    await s3.deleteObject(params).promise();
    logInfo('Objeto removido', req, { bucketName, fileName });
    res.status(200).json({ message: 'Arquivo deletado com sucesso' });
  } catch (error) {
    logError('Erro ao remover objeto', req, error);
    res.status(500).json({ error: 'Erro ao deletar arquivo', details: error });
  }
});

/**
 * @swagger
 * /buckets/replicar/{fileName}:
 *   post:
 *     tags:
 *       - Buckets
 *     summary: Replicar arquivo de bucket principal para secundário
 *     parameters:
 *       - in: path
 *         name: fileName
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Arquivo replicado com sucesso
 */
app.post('/buckets/replicar/:fileName', async (req, res) => {
  const sourceBucket = `${process.env.RA}-dsm-vot-prod`;
  const destinationBucket = `${process.env.RA}-dsm-vot-hml`;
  const { fileName } = req.params;

  try {
    const file = await s3.getObject({ Bucket: sourceBucket, Key: fileName }).promise();
    const result = await s3.upload({
      Bucket: destinationBucket,
      Key: fileName,
      Body: file.Body,
    }).promise();

    logInfo('Arquivo replicado com sucesso', req, result);
    res.status(200).json({ message: 'Arquivo replicado com sucesso', result });
  } catch (error) {
    logError('Erro ao replicar arquivo', req, error);
    res.status(500).json({ error: 'Erro ao replicar arquivo', details: error });
  }
});
//#endregion

// Inicializa o Swagger (depois de todas as rotas)
swaggerDocs(app);

// Start server
app.listen(3000, () => {
  console.log('Servidor rodando em http://localhost:3000');
  console.log('Swagger disponível em http://localhost:3000/swagger');
});
