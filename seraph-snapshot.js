const seraph = require('seraph');
const async = require('async');
const _ = require('underscore');

const fetchdb = (id) => {
  const cypher = `MATCH (node) WHERE node.graph_id = "${id}" OPTIONAL MATCH (node)-[rel]-(x) RETURN node, labels(node) as labels, collect(rel) as rels;`;
  return cypher;
};

function dbToJSON(db, id, cb) {
  db.query(fetchdb(id), (err, data) => {
    if (err) return cb(err);
    const normalized = {};
    data.forEach(row => {
      normalized[`node_${row.node.id}`] = {
        type: 'node',
        labels: row.labels,
        data: row.node
      };
      row.rels.forEach(rel => {
        normalized[`rel_${rel.id}`] = {
          type: 'rel',
          data: rel
        };
      });
    });
    cb(null, normalized);
  });
}

function JSONtoStatementList(data) {
  const keys = Object.keys(data);
  const nodes = keys.filter(key => data[key].type == 'node');
  const rels = keys.filter(key => data[key].type == 'rel');
  const keymap = {};
  // can't use query params as we're exporting to a single string
  function createParamString(obj) {
    const kvs = Object.keys(obj).map(key => `\`${key}\`:${JSON.stringify(obj[key])}`).join(',');
    return `{${kvs}}`;
  }
  let uniqueNameIndex = 0;
  function createUniqueName(type) {
    return type + (uniqueNameIndex++);
  }
  const creates = [];
  nodes.forEach(nodeKey => {
    const node = data[nodeKey];
    delete node.data.id;
    const name = keymap[nodeKey] = createUniqueName('node');
    const params = createParamString(node.data);
    const labels = node.labels.map(l => `:\`${l}\``).join('');
    creates.push({ 
      statement: `(${name}${labels} ${params})`, 
      refs:[],
      id: name
    });
  });
  rels.forEach(relKey => {
    const rel = data[relKey];
    const name = keymap[relKey] = createUniqueName('rel');
    const params = createParamString(rel.data.properties);
    const start = keymap[`node_${rel.data.start}`];
    const finish = keymap[`node_${rel.data.end}`];
    const type = `\`${rel.data.type}\``;
    creates.push({
      statement: `(${start})-[${name}:${type} ${params}]->(${finish})`,
      refs:[start,finish],
      id: name
    }); 
  });
  return creates;
}

function restoreTransactional(db, data, cb) {
  const statements = JSONtoStatementList(data);
  const stepSize = 15;
  const nodeMap = {};
  const groups = [];
  let txn;
  
  for (let i = 0; i < statements.length; i += 15) {
    groups.push(statements.slice(i, i + 15));
  }

  async.forEachSeries(groups, (s, cb) => {
    const start = _.chain(s).pluck('refs').flatten().uniq().map(ref => `${ref}=node(${nodeMap[ref]})`).value().join(',');
    let query = start ? `START ${start}` : '';
    query += ` CREATE ${s.map(row => row.statement).join(',')}`;
    query += ` RETURN ${s.map(row => 'id(' + row.id + ') as ' + row.id).join(',')}`;
    const endpoint = txn || 'transaction';
    const op = db.operation(endpoint, 'POST', {
      statements: [{ statement:query }]
    });
    db.call(op, (err, result, transLoc) => {
      if (err) return cb(err);
      // see how enterprise.
      if (!txn) txn = require('url').parse(transLoc).path.replace(`${db.options.endpoint}/`, '');
      const keys = result.results[0].columns;
      const vals = result.results[0].data[0].row;
      keys.forEach((key, i) => {
        nodeMap[key] = vals[i];
      });

      cb();
    });
  }, e => {
    if (e) return cb(e);
    const op = db.operation(`${txn}/commit`, 'POST', {statements:[]});
    db.call(op, cb);
  });
}

function JSONtoCypher(data) {
  return `CREATE ${JSONtoStatementList(data).map(s => s.statement).join(',')}`
}

module.exports = {
  json: dbToJSON,
  jsonToCypher: JSONtoCypher,
  restoreTransactional: restoreTransactional,
  cypher(db, id, cb) {
    dbToJSON(db, id, (err, data) => {
      if (err) return cb(err);
      else cb(null, JSONtoCypher(data));
    });
  }
}