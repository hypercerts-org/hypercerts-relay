import * as plc from '@did-plc/server'

const port = Number(process.env.PORT ?? 2582)
const database = plc.Database.mock()
const server = plc.PlcServer.create({ db: database, port })

await server.start()
console.log(`acceptance PLC listening on :${port}`)

const stop = async () => {
  await server.destroy()
  process.exit(0)
}
process.once('SIGINT', stop)
process.once('SIGTERM', stop)
