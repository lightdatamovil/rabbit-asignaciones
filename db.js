import redis from 'redis';
import dotenv from 'dotenv';
import { logRed } from './src/funciones/logsCustom.js';

dotenv.config({ path: process.env.ENV_FILE || ".env" });

const redisHost = process.env.REDIS_HOST;
const redisPort = process.env.REDIS_PORT;
const redisPassword = process.env.REDIS_PASSWORD;

const databaseHost = process.env.DATABASE_HOST;
const databasePort = process.env.DATABASE_PORT;
const databaseUser = process.env.DATABASE_USER;
const databasePassword = process.env.DATABASE_PASSWORD;
const databaseName = process.env.DATABASE_NAME;

export const redisClient = redis.createClient({
    socket: {
        host: redisHost,
        port: redisPort,
    },
    password: redisPassword,
});

redisClient.on('error', (err) => {
    logRed(`Error al conectar con Redis: ${err.message} `)

});

export async function updateRedis(empresaId, envioId, choferId) {
    let DWRTE = await redisClient.get('DWRTE');

    DWRTE = DWRTE ? JSON.parse(DWRTE) : {};
    const empresaKey = `e.${empresaId}`;
    const envioKey = `en.${envioId}`;

    // Si la empresa no existe, la creamos
    if (!DWRTE[empresaKey]) {
        DWRTE[empresaKey] = {};
    }

    // Si el envío no existe, lo creamos
    if (!DWRTE[empresaKey][envioKey]) {
        DWRTE[empresaKey][envioKey] = {};
    }

    // Actualizamos el choferId siempre
    DWRTE[empresaKey][envioKey].choferId = choferId;

    await redisClient.set('DWRTE', JSON.stringify(DWRTE));
}


let companiesList = {};

export function getDbConfig() {
    return {
        host: databaseHost,
        user: databaseUser,
        password: databasePassword,
        database: databaseName,
        port: databasePort
    };
}

export function getProdDbConfig(company) {
    return {
        host: "bhsmysql1.lightdata.com.ar",
        user: company.dbuser,
        password: company.dbpass,
        database: company.dbname
    };
}

async function loadCompaniesFromRedis() {
    try {
        const companiesListString = await redisClient.get('empresasData');

        companiesList = JSON.parse(companiesListString);

    } catch (error) {
        logRed(`Error en loadCompaniesFromRedis: ${error.message} `)

        throw error;
    }
}

export async function getCompanyById(companyId) {
    try {
        let company = companiesList[companyId];

        if (company == undefined || Object.keys(companiesList).length === 0) {
            try {
                await loadCompaniesFromRedis();

                company = companiesList[companyId];
            } catch (error) {
                logRed(`Error al cargar compañías desde Redis: ${error.message} `)

                throw error;
            }
        }

        return company;
    } catch (error) {
        logRed(`Error en getCompanyById: ${error.message} `)

        throw error;
    }
}

export async function executeQuery(dbConnection, query, values) {
    try {
        return new Promise((resolve, reject) => {
            dbConnection.query(query, values, (err, results) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(results);
                }
            });
        });
    } catch (error) {
        logRed(`Error al ejecutar la query: ${error.message} `)

        throw error;
    }
}
