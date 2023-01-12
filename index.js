const mysql = require("mysql2");
const { Client } = require("@elastic/elasticsearch");
const client = new Client({
  node: "http://localhost:9200",
});

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  database: "org",
  password: "password",
});

async function updateElastic(m) {
  const update = {
    script: {
      source: `
              ctx._source.callInfo.callTime.talkTime = ${m.agent_talktime_sec};
            `,
    },
    query: {
      bool: {
        must: {
          match: {
            "callInfo.agentLegUuid.keyword": `${m.cdrid}`,
          },
        },
      },
    },
  };
  client
    .updateByQuery({
      index: "deliveriesdevlogger",
      body: update,
    })
    .then(
      (res) => {
        console.log("Success", res);
      },
      (err) => {
        console.log("Error", err);
      }
    );
}

async function streamSQLData() {
  const stream = connection.query("SELECT * FROM disposecall").stream();
  stream.on("data", async (m) => {
    await updateElastic(m);
  });

  stream.on("end", () => {
    // all rows have been received
    console.log("All rows have been received");
  });
}

streamSQLData();

connection.end();
