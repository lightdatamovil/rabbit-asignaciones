
export function verifyParamaters(body, parametrosRequeridos, userData = false) {
    const param = ['deviceId', 'appVersion', 'brand', 'model', 'androidVersion', ...parametrosRequeridos];
    if (userData) {
        param.push('companyId', 'userId', 'profile');
    }

    const faltantes = param.filter(p => !body[p]);

    if (faltantes.length > 0) {
        return `Faltan los siguientes parámetros: ${faltantes.join(', ')}`;
    }

    return null;
};