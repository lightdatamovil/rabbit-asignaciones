import express from 'express';
import { json } from 'body-parser';
import routes from './routes/empresaRoute';

const app = express();
app.use(json());

app.use("/", routes);

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`Servidor corriendo en el puerto ${PORT}`);
});
