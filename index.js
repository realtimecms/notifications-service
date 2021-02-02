const App = require("@live-change/framework")
const app = new App()
const validators = require("../validation")

require('../../i18n/ejs-require.js')
const i18n = require('../../i18n')
const purify = require('../config/purify.js')

const definition = app.createServiceDefinition({
  name: "notifications",
  eventSourcing: true,
  validators
})

const { getAccess, hasRole, checkIfRole, getPublicInfo } =
    require("../access-control-service/access.js")(app, definition)

const User = definition.foreignModel('users', 'User')
const Session = definition.foreignModel('session', 'Session')
const PublicSessionInfo = definition.foreignModel('accessControl', 'PublicSessionInfo')

const config = require('../config/notifications.js')(definition)


const Notification = definition.model({
  name: "Notification",
  properties: {
    session: {
      type: Session,
    },
    user: {
      type: User,
    },
    time: {
      type: Date,
      validation: ['nonEmpty']
    },
    state: {
      type: String
    },
    readState: {
      type: String,
      defaultValue: 'new'
    },
    emailState: {
      type: String,
      defaultValue: 'new'
    },
    ...config.fields
  },
  indexes: {
    userNotifications: {
      property: ["user", "time"]
    },
    userNotificationsByReadState: {
      property: ["user", "readState"]
    },
    userNotificationsByEmailState: {
      property: ["user", "emailState"]
    },
    sessionNotifications: {
      property: ["session", "time"]
    },
    sessionNotificationsByReadState: {
      property: ["session", "readState"]
    },
    userUnreadNotificationsCount: { /// For counting
      function: async function(input, output) {
        await input.table('notifications_Notification').onChange(
          (obj, oldObj, id, ts) => {
            const unread = obj && obj.user && obj.readState == 'new'
            const oldUnread = oldObj && oldObj.user && oldObj.readState == 'new'
            if(unread && !oldUnread) { // now unread
              output.update(obj.user, [
                { op: "conditional",
                  conditions: [
                    { test: 'notExist', property: 'count' }
                  ],
                  operations: [
                    { op: 'set', property: 'count', value: 1 },
                    { op: 'set', property: 'lastUpdate', value: ts }
                  ]
                },
                { op: "conditional",
                  conditions: [
                    { test: 'lt', property: 'lastUpdate', value: ts }
                  ],
                  operations: [
                    { op: 'add', property: 'count', value: 1 }
                  ]
                },
                { op: 'merge', value: { severity: obj.severity, scan: obj.scan, lastUpdate: ts } },
              ])
            } else if(!unread && oldUnread) { // been unread
              output.update(oldObj.user, [
                { op: "conditional",
                  conditions: [
                    { test: 'lt', property: 'lastUpdate', value: ts }
                  ],
                  operations: [
                    { op: 'add', property: 'count', value: -1 }
                  ]
                }
              ])
            }
          }
        )
      }
    },
    sessionUnreadNotificationsCount: { /// For counting
      function: async function(input, output) {
        await input.table('notifications_Notification').onChange(
            (obj, oldObj, id, ts) => {
              const unread = obj && obj.session && obj.readState == 'new'
              const oldUnread = oldObj && oldObj.session && oldObj.readState == 'new'
              if(unread && !oldUnread) { // now unread
                output.update(obj.session, [
                  { op: "conditional",
                    conditions: [
                      { test: 'notExist', property: 'count' }
                    ],
                    operations: [
                      { op: 'set', property: 'count', value: 1 },
                      { op: 'set', property: 'lastUpdate', value: ts }
                    ]
                  },
                  { op: "conditional",
                    conditions: [
                      { test: 'lt', property: 'lastUpdate', value: ts }
                    ],
                    operations: [
                      { op: 'add', property: 'count', value: 1 }
                    ]
                  },
                  { op: 'merge', value: { severity: obj.severity, scan: obj.scan, lastUpdate: ts } },
                ])
              } else if(!unread && oldUnread) { // been unread
                output.update(oldObj.session, [
                  { op: "conditional",
                    conditions: [
                      { test: 'lt', property: 'lastUpdate', value: ts }
                    ],
                    operations: [
                      { op: 'add', property: 'count', value: -1 }
                    ]
                  }
                ])
              }
            }
        )
      }
    }

  },
  crud: {
    deleteTrigger: true,
    options: { /// Crud only for admins
      access: (params, {client, service}) => { /// is it really needed?
        return client.roles && client.roles.includes('admin')
      }
    }
  }
})

definition.event({
  name: "marked",
  async execute({ notification, state }) {
    if(state === 'read'){
      await Notification.update(notification, { state: state, readState: state })
    } else {
      await Notification.update(notification, { state })
    }
  }
})

definition.event({
  name: "readState",
  async execute({ notification, readState }) {
    await Notification.update(notification, { readState: readState })
  }
})

definition.event({
  name: "allRead",
  async execute({ user, session }) {
    const update = { readState: 'read' }
    const prefix = user
        ? JSON.stringify(user) + ':"new"_'
        : JSON.stringify(session) + ':"new"_'
    console.log("MARK ALL AS READ PREFIX", prefix)
    await app.dao.request(['database', 'query'], app.databaseName, `(${
        async (input, output, { tableName, indexName, update, range }) => {
          await input.index(indexName).range(range).onChange((obj, oldObj) => {
            if(obj) output.table(tableName).update(obj.to, [{ op: 'merge', value: update }])
          })
        }
    })`, {
      tableName: Notification.tableName,
      indexName: user
          ? Notification.tableName + "_userNotificationsByReadState"
          : Notification.tableName + "_sessionNotificationsByReadState",
      update,
      range: {
        gte: prefix,
        lte: prefix + "\xFF\xFF\xFF\xFF"
      }
    })
  }
})

definition.event({
  name: "allRemoved",
  async execute({ user, session }) {
    const prefix = user
        ? JSON.stringify(user) + ':"new"_'
        : JSON.stringify(session) + ':"new"_'
    console.log("MARK ALL AS READ PREFIX", prefix)
    await app.dao.request(['database', 'query'], app.databaseName, `(${
        async (input, output, { tableName, indexName, update, range }) => {
          await input.index(indexName).range(range).onChange((obj, oldObj) => {
            if(obj) output.table(tableName).delete(obj.to)
          })
        }
    })`, {
      tableName: Notification.tableName,
      indexName: user
          ? Notification.tableName + "_userNotificationsByReadState"
          : Notification.tableName + "_sessionNotificationsByReadState",
      range: {
        gte: prefix,
        lte: prefix + "\xFF\xFF\xFF\xFF"
      }
    })
  }
})

definition.event({
  name: "removed",
  async execute({ notification }) {
    await Notification.delete(notification)
  }
})

definition.event({
  name: "emailNotification",
  async execute({ user, notifications }) {
    await Promise.all(notifications.map(notification => Notification.update(notification, { emailState: 'sent' })))
  }
})

definition.view({
  name: "myNotifications",
  properties: {
    gt: {
      type: String,
    },
    lt: {
      type: String,
    },
    gte: {
      type: String,
    },
    lte: {
      type: String,
    },
    limit: {
      type: Number
    },
    reverse: {
      type: Boolean
    }
  },
  returns: {
    type: Array,
    of: {
      type: Notification
    }
  },
  autoSlice: true,
  access: (params, { client }) => !!client.user, // only for logged in
  async daoPath({ gt, lt, gte, lte, limit, reverse }, {client, service}, method) {
    const [index, prefix] = client.user
        ? ['userNotifications', `"${client.user}"`]
        : ['sessionNotifications', `"${(await getPublicInfo(client.sessionId)).id}"`]
    if(!Number.isSafeInteger(limit)) limit = 100
    function getPrefix(id) {
      if(id === '') return `${prefix}:`
      if(id === '\xFF\xFF\xFF\xFF') return `${prefix}:\xFF\xFF\xFF\xFF`
      return `${prefix}:"${id.match(/":"([0-9-]+T[0-9:]+.[0-9]+Z)"_/)[1]}"_`
    }
    const range = {
      gt: (typeof gt == 'string') ? getPrefix(gt)+"\xFF\xFF\xFF\xFF" : undefined,
      lt: (typeof lt == 'string') ? getPrefix(lt) : undefined,
      gte: (typeof gte == 'string') ? getPrefix(gte) : (typeof gt == 'string' ? undefined : `${prefix}`),
      lte: (typeof lte == 'string')
          ? getPrefix(lte)+"\xFF\xFF\xFF\xFF"
          : (typeof lt == 'string' ? undefined : `${prefix}:\xFF\xFF\xFF\xFF`),
      limit,
      reverse
    }
    const notifications = await Notification.sortedIndexRangeGet(index, range)
    console.log("MESSAGES RANGE", JSON.stringify({ user: client.user, gt, lt, gte, lte, limit, reverse }) ,
        "\n  TO", JSON.stringify(range),
        "\n  RESULTS", notifications.length, notifications.map(m => m.id))
    return Notification.sortedIndexRangePath(index, range)
  }
})

definition.view({
  name: "myUnreadCount",
  properties: {

  },
  returns: {
    type: Object
  },
  async daoPath({ }, { client, service }, method) {
    const [index, id] = client.user
        ? ['userUnreadNotificationsCount', `${client.user}`]
        : ['sessionUnreadNotificationsCount', `${(await getPublicInfo(client.sessionId)).id}`]
    console.log("UNREAD", index, id)
    return ['database', 'indexObject', app.databaseName, 'notifications_Notification_'+index, id]
  }
})

definition.trigger({
  name: "Notify",
  properties: {
    user: {
      type: User,
    },
    session: {
      type: PublicSessionInfo,
    },
    notificationType: {
      type: String,
      validation: ['nonEmpty']
    },
    ...config.fields
  },
  async execute(params , { service }, emit) {
    const { user, session } = params
    if(!user && !session) throw new Error("session or user required")
    const notification = app.generateUid()
    const time = new Date()
    let data = {}
    for(const key in config.fields) data[key] = params[key]
    emit({
      type: "NotificationCreated",
      notification,
      data: { ...data, user, session, time, readState: 'new' }
    })
    if(user) {
      const emailNotificationTimestamp = Date.now() + config.emailNotificationDelay + config.emailNotificationCheckDelay
      await app.trigger({
        type: 'createTimer',
        timer: {
          timestamp: emailNotificationTimestamp,
          service: 'notifications',
          trigger: {
            type: 'checkEmailNotificationState',
            user
          }
        }
      })
      console.log("SET CHECK NOTIFICATIONS TIMER FIRE AT", new Date(emailNotificationTimestamp))
    }
    return notification
  }
})

definition.trigger({
  name: 'checkEmailNotificationState',
  properties: {
    user: {
      type: User
    }
  },
  waitForEvents: true,
  queuedBy: ['user'],
  async execute({ user }, { service }, emit) {
    console.log("STARTED EMAIL CHECK!", user)
    const notifications =  await Notification.sortedIndexRangeGet(
        'userNotificationsByEmailState', [user, 'new'])
    if(notifications.length == 0) return // already notified about everything

    const userEntity = await User.get(user)
    const userData = { ...userEntity.userData, display: userEntity.display }

    console.log("GOT NOTIFICATIONS!", notifications)
    const renderResults = await Promise.all(notifications.map(async notification => {
      const rendered = await app.trigger({
        type: "renderNotification_" + notification.notificationType,
        notification: {
          ...notification,
          id: notification.to
        },
        user: userData
      })
      console.log("GOT RENDERED", rendered)
      return rendered.map(render => ({ ...notification, ...render }))
    }))
    console.log("RENDER RESULTS", renderResults)
    const rendered = renderResults.flat().filter(n => !!n) // remove nulls
    if(rendered.length > 0) {
      const lang = user.language || Object.keys(i18n.languages)[0]
      const email = i18n.languages[lang].emailNotifications.notificationsEmail({
        user: userData,
        email: userData.email,
        notifications: rendered,
        purify
      })
      console.log("SENDING EMAIL!", email)
      await service.trigger({
        type:"sendEmail",
        email
      })
      console.log("EMAIL SENT!")
    } else {
      console.log("NO EMAIL NEEDED!")
    }
    console.log("MARKING NOTIFICATIONS AS SENT")
    emit({
      type: "emailNotification",
      user, notifications: rendered.map(notification => notification.to)
    })
  }
})

definition.action({
  name: "mark",
  properties: {
    notification: {
      type: Notification
    },
    state: {
      type: String
    }
  },
  access: async ({ notification }, { client, visibilityTest }) => {
    if(!client.user) return false
    if(visibilityTest) return true
    const notificationRow = await Notification.get(notification)
    if(!notificationRow) throw 'notFound'
    return client.user
        ? notificationRow.user == client.user
        : notificationRow.session == (await getPublicInfo(client.sessionId)).id
  },
  async execute({ notification, state }, { client, service }, emit) {
    emit({
      type: "marked",
      notification,
      state
    })
  }
})

definition.action({
  name: "toggleReadStatus",
  properties: {
    notification: {
      type: Notification
    },
    read: {
      type: Boolean
    }
  },
  access: async ({ notification }, { client, visibilityTest }) => {
    if(!client.user) return false
    if(visibilityTest) return true
    const notificationRow = await Notification.get(notification)
    if(!notificationRow) throw 'notFound'
    return client.user
        ? notificationRow.user == client.user
        : notificationRow.session == (await getPublicInfo(client.sessionId)).id
  },
  async execute({ notification, readState }, { client, service }, emit) {
    emit({
      type: "readState",
      notification,
      readState
    })
  }
})

definition.action({
  name: "markAllAsRead",
  properties: {
  },
  access: async ({}, { client, visibilityTest }) => {
    if(!client.user) return false
    if(visibilityTest) return true
    return true
  },
  async execute({ notification, readState }, { client, service }, emit) {
    const user = client.user
    const session = (await getPublicInfo(client.sessionId)).id
    console.log("MARK ALL AS READ!!", user, session)
    emit({
      type: "allRead",
      user,
      session
    })
  }
})

definition.action({
  name: "remove",
  properties: {
    notification: {
      type: Notification
    }
  },
  access: async ({ notification }, { client, visibilityTest }) => {
    if(!client.user) return false
    if(visibilityTest) return true
    const notificationRow = await Notification.get(notification)
    if(!notificationRow) throw 'notFound'
    return client.user
        ? notificationRow.user == client.user
        : notificationRow.session == (await getPublicInfo(client.sessionId)).id
  },
  async execute({ notification }, { client, service }, emit) {
    emit({
      type: "removed",
      notification
    })
  }
})

definition.action({
  name: "removeAll",
  properties: {
  },
  access: async ({}, { client, visibilityTest }) => {
    if(!client.user) return false
    if(visibilityTest) return true
    return true
  },
  async execute({ notification, readState }, { client, service }, emit) {
    const user = client.user
    const session = (await getPublicInfo(client.sessionId)).id
    console.log("REMOVE ALL!!", user, session)
    emit({
      type: "allRemoved",
      user,
      session
    })
  }
})

module.exports = definition

async function start() {
  process.on('unhandledRejection', (reason, p) => {
    console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
  })

  app.processServiceDefinition(definition, [ ...app.defaultProcessors ])
  await app.updateService(definition)//, { force: true })
  const service = await app.startService(definition, { runCommands: true, handleEvents: true })

}

if (require.main === module) start().catch( error => { console.error(error); process.exit(1) })