import fs from "fs/promises";
import levelUp from "levelup";
import leveldown from "leveldown";
import sub from "subleveldown";
import { DataMock } from "@pawel-up/data-mock";
import { performance } from "perf_hooks";

/** @typedef {import('abstract-leveldown').AbstractLevelDOWN} AbstractLevelDOWN */
/** @typedef {import('abstract-leveldown').PutBatch } PutBatch  */
/** @typedef {import('abstract-leveldown').AbstractIteratorOptions } AbstractIteratorOptions  */
/** @typedef {import('levelup').LevelUp} LevelUp */
/** @typedef {import('leveldown').LevelDownIterator} LevelDownIterator */
/** @typedef {import('leveldown').LevelDown} LevelDown */
/** @typedef {import('leveldown').Bytes} Bytes */
/** @typedef {import('perf_hooks').PerformanceEntry} PerformanceEntry */
/** @typedef {import('./types').QueryOptions} QueryOptions */
/** @typedef {import('./types').IndexQueryOptions} IndexQueryOptions */
/** @typedef {import('./types').IndexQueryResult} IndexQueryResult */
/** @typedef {import('./types').IndexKeysResult} IndexKeysResult */


const dbPath = "./data/";

/**
 * @param {number[]} values
 */
function median(values) {
  if (values.length === 0) {
    return 0;
  }
  values.sort((a, b) => a - b);
  const half = Math.floor(values.length / 2);
  if (values.length % 2) {
    return values[half];
  }
  return (values[half - 1] + values[half]) / 2.0;
}

class TestHistory {
  constructor() {
    /**
     * @type LevelUp
     * @private
     */
    this.db = undefined;
    /**
     * @type LevelUp
     * @private
     */
    this.history = undefined;
    /**
     * @type LevelUp
     * @private
     */
    this.historyApp = undefined;
    /**
     * @type LevelUp
     * @private
     */
    this.historyUser = undefined;
    /**
     * @type LevelUp
     * @private
     */
    this.historyData = undefined;

    this.apps = ["b8dbdb61", "c4ef15e4", "24ec7eb1", "06af98c0", "a2a156fb"];
    this.users = [
      "2289ce90-d2ab-4e34-8095-3de3a9e3c83a",
      "cf9084a2-c050-407a-b45a-816955c9df26",
      "e7fa7911-0db3-4f0c-b1ea-9439073d828a",
      "3b52ebcf-b97f-41fc-8185-946fd2fceb97",
    ];
    this.mock = new DataMock();
    this.queryAll = process.argv.includes("--all");
    this.queryApp = process.argv.includes("--app");
    this.queryUser = process.argv.includes("--user");
    this.detailedQuery = process.argv.includes("--detailed");
    this.generateData = process.argv.includes("--generate");
    if (!this.queryAll && !this.queryApp && !this.queryUser && !this.generateData) {
      console.error("Specify either --all, --app, or --user.");
      process.exit(1);
    }
  }

  async start() {
    await this.init();
    if (this.generateData) {
      await this.clear();
      await this.addHistory();
    }
    if (this.queryAll) {
      await this.measureByTime();
    }
    if (this.queryApp) {
      await this.measureByApp(this.mock.random.pickOne(this.apps));
    }
    if (this.queryUser) {
      await this.measureByUser(this.mock.random.pickOne(this.users));
    }
    this.printReport();
  }

  printReport() {
    // @ts-ignore
    const measures = performance.getEntriesByType("measure");
    if (this.queryAll) {
      this.printAllReport(measures);
    }
    if (this.queryApp) {
      this.printAppReport(measures);
    }
    if (this.queryUser) {
      this.printUserReport(measures);
    }
  }

  /**
   * @param {PerformanceEntry[]} measures
   */
  printAllReport(measures) {
    const traditionalKey = [];
    const traditionalValue = [];
    const keyFilter = [];
    const index = [];
    let traditionalKeyTotal = 0;
    let traditionalValueTotal = 0;
    let keyFilterTotal = 0;
    let indexTotal = 0;
    measures.forEach((item) => {
      if (item.name.startsWith("time-trad-keys-p-")) {
        traditionalKey.push(item.duration);
      } else if (item.name === "time-trad-keys-all") {
        traditionalKeyTotal = item.duration;
      } else if (item.name.startsWith("time-trad-values-p-")) {
        traditionalValue.push(item.duration);
      } else if (item.name === "time-trad-values-all") {
        traditionalValueTotal = item.duration;
      } else if (item.name.startsWith("time-key-filter-p-")) {
        keyFilter.push(item.duration);
      } else if (item.name === "time-key-filter-all") {
        keyFilterTotal = item.duration;
      } else if (item.name.startsWith("time-index-p-")) {
        index.push(item.duration);
      } else if (item.name === "time-index-all") {
        indexTotal = item.duration;
      }
    });
    console.log('');
    console.log("List by time (traditional iterator, key filtering)");
    console.log("Minimum (ms): ", Math.min(...traditionalKey));
    console.log("Maximum (ms): ", Math.max(...traditionalKey));
    console.log("Median (ms): ", median([...traditionalKey]));
    console.log("Total time (ms): ", traditionalKeyTotal);
    if (this.detailedQuery) {
      console.log(traditionalKey);
    }
    console.log("\n=====================\n");
    console.log("List by time (traditional iterator, value filtering)");
    console.log("Minimum (ms): ", Math.min(...traditionalValue));
    console.log("Maximum (ms): ", Math.max(...traditionalValue));
    console.log("Median (ms): ", median([...traditionalValue]));
    console.log("Total time (ms): ", traditionalValueTotal);
    if (this.detailedQuery) {
      console.log(traditionalValue);
    }
    console.log("\n=====================\n");
    console.log("List by time (key filtering)");
    console.log("Minimum (ms): ", Math.min(...keyFilter));
    console.log("Maximum (ms): ", Math.max(...keyFilter));
    console.log("Median (ms): ", median([...keyFilter]));
    console.log("Total time (ms): ", keyFilterTotal);
    if (this.detailedQuery) {
      console.log(keyFilter);
    }
    console.log("\n=====================\n");
    console.log("List by time (index table)");
    console.log("Minimum (ms): ", Math.min(...index));
    console.log("Maximum (ms): ", Math.max(...index));
    console.log("Median (ms): ", median([...index]));
    console.log("Total time (ms): ", indexTotal);
    if (this.detailedQuery) {
      console.log(index);
    }
  }

  /**
   * @param {PerformanceEntry[]} measures
   */
  printAppReport(measures) {
    const traditionalKey = [];
    const traditionalValue = [];
    const keyFilter = [];
    const index = [];
    let traditionalKeyTotal = 0;
    let traditionalValueTotal = 0;
    let keyFilterTotal = 0;
    let indexTotal = 0;
    measures.forEach((item) => {
      if (item.name.startsWith("app-time-trad-keys-p-")) {
        traditionalKey.push(item.duration);
      } else if (item.name === "app-time-trad-keys-all") {
        traditionalKeyTotal = item.duration;
      } else if (item.name.startsWith("app-time-trad-values-p-")) {
        traditionalValue.push(item.duration);
      } else if (item.name === "app-time-trad-values-all") {
        traditionalValueTotal = item.duration;
      } else if (item.name.startsWith("app-time-key-filter-p-")) {
        keyFilter.push(item.duration);
      } else if (item.name === "app-time-key-filter-all") {
        keyFilterTotal = item.duration;
      } else if (item.name.startsWith("app-time-index-p-")) {
        index.push(item.duration);
      } else if (item.name === "app-time-index-all") {
        indexTotal = item.duration;
      }
    });
    console.log('');
    console.log("List by application (traditional iterator, key filtering)");
    console.log("Minimum (ms): ", Math.min(...traditionalKey));
    console.log("Maximum (ms): ", Math.max(...traditionalKey));
    console.log("Median (ms): ", median([...traditionalKey]));
    console.log("Total time (ms): ", traditionalKeyTotal);
    if (this.detailedQuery) {
      console.log(traditionalKey);
    }
    console.log("\n=====================\n");
    console.log("List by application (traditional iterator, value filtering)");
    console.log("Minimum (ms): ", Math.min(...traditionalValue));
    console.log("Maximum (ms): ", Math.max(...traditionalValue));
    console.log("Median (ms): ", median([...traditionalValue]));
    console.log("Total time (ms): ", traditionalValueTotal);
    if (this.detailedQuery) {
      console.log(traditionalValue);
    }
    console.log("\n=====================\n");
    console.log("List by application (key filtering)");
    console.log("Minimum (ms): ", Math.min(...keyFilter));
    console.log("Maximum (ms): ", Math.max(...keyFilter));
    console.log("Median (ms): ", median([...keyFilter]));
    console.log("Total time (ms): ", keyFilterTotal);
    if (this.detailedQuery) {
      console.log(keyFilter);
    }
    console.log("\n=====================\n");
    console.log("List by application (index table)");
    console.log("Minimum (ms): ", Math.min(...index));
    console.log("Maximum (ms): ", Math.max(...index));
    console.log("Median (ms): ", median([...index]));
    console.log("Total time (ms): ", indexTotal);
    if (this.detailedQuery) {
      console.log(index);
    }
  }

  /**
   * @param {PerformanceEntry[]} measures
   */
  printUserReport(measures) {
    const traditionalKey = [];
    const traditionalValue = [];
    const keyFilter = [];
    const index = [];
    let traditionalKeyTotal = 0;
    let traditionalValueTotal = 0;
    let keyFilterTotal = 0;
    let indexTotal = 0;
    measures.forEach((item) => {
      if (item.name.startsWith("user-time-trad-keys-p-")) {
        traditionalKey.push(item.duration);
      } else if (item.name === "user-time-trad-keys-all") {
        traditionalKeyTotal = item.duration;
      } else if (item.name.startsWith("user-time-trad-values-p-")) {
        traditionalValue.push(item.duration);
      } else if (item.name === "user-time-trad-values-all") {
        traditionalValueTotal = item.duration;
      } else if (item.name.startsWith("user-time-key-filter-p-")) {
        keyFilter.push(item.duration);
      } else if (item.name === "user-time-key-filter-all") {
        keyFilterTotal = item.duration;
      } else if (item.name.startsWith("user-time-index-p-")) {
        index.push(item.duration);
      } else if (item.name === "user-time-index-all") {
        indexTotal = item.duration;
      }
    });
    console.log('');
    console.log("List by user (traditional iterator, key filtering)");
    console.log("Minimum (ms): ", Math.min(...traditionalKey));
    console.log("Maximum (ms): ", Math.max(...traditionalKey));
    console.log("Median (ms): ", median([...traditionalKey]));
    console.log("Total time (ms): ", traditionalKeyTotal);
    if (this.detailedQuery) {
      console.log(traditionalKey);
    }
    console.log("\n=====================\n");
    console.log("List by user (traditional iterator, value filtering)");
    console.log("Minimum (ms): ", Math.min(...traditionalValue));
    console.log("Maximum (ms): ", Math.max(...traditionalValue));
    console.log("Median (ms): ", median([...traditionalValue]));
    console.log("Total time (ms): ", traditionalValueTotal);
    if (this.detailedQuery) {
      console.log(traditionalValue);
    }
    console.log("\n=====================\n");
    console.log("List by user (key filtering)");
    console.log("Minimum (ms): ", Math.min(...keyFilter));
    console.log("Maximum (ms): ", Math.max(...keyFilter));
    console.log("Median (ms): ", median([...keyFilter]));
    console.log("Total time (ms): ", keyFilterTotal);
    if (this.detailedQuery) {
      console.log(keyFilter);
    }
    console.log("\n=====================\n");
    console.log("List by user (index table)");
    console.log("Minimum (ms): ", Math.min(...index));
    console.log("Maximum (ms): ", Math.max(...index));
    console.log("Median (ms): ", median([...index]));
    console.log("Total time (ms): ", indexTotal);
    if (this.detailedQuery) {
      console.log(index);
    }
  }

  /**
   * @private
   */
  async init() {
    await fs.mkdir(dbPath, { recursive: true });
    // @ts-ignore
    this.db = levelUp(leveldown(dbPath));
    this.history = sub(this.db, "history");
    this.historyApp = sub(this.db, "history-app");
    this.historyUser = sub(this.db, "history-user");
    this.historyData = sub(this.db, "history-data");
  }

  /**
   * @private
   */
  async clear() {
    await this.history.clear();
    await this.historyApp.clear();
    await this.historyData.clear();
    await this.historyUser.clear();
  }

  /**
   * @private
   */
  async addHistory() {
    console.log("Creating the history...");
    for (let j = 0; j < 10; j ++) {
      await this.addHistoryBatch(j);
    }
    console.log("The history is ready.");
  }

  /**
   * @param {number} index 
   * @param {number=} size 
   */
  async addHistoryBatch(index, size=10000) {
    const { history, historyApp, historyData, historyUser, mock, apps, users } = this;
    const now = Date.now();
    let singleList = /** @type PutBatch[] */ ([]);
    let dataList = /** @type PutBatch[] */ ([]);
    let userList = /** @type PutBatch[] */ ([]);
    let appList = /** @type PutBatch[] */ ([]);
    for (let i = 0; i < size; i++) {
      const request = this.generateRequest();
      const d = mock.types.datetime({ min: now - 1000000000, max: now });
      const time = d.toJSON();
      const app = mock.random.pickOne(apps);
      const user = mock.random.pickOne(users);
      const singleDbKey = `${time}~${app}~${user}`;
      const dataKey = `${time}~${user}`;
      const userKey = `~${user}~${time}~`;
      const appKey = `~${app}~${time}~`;
      singleList.push({
        key: singleDbKey,
        type: "put",
        value: JSON.stringify({
          key: singleDbKey,
          app,
          user,
          date: d.toISOString(),
          request,
        })
      });
      dataList.push({
        key: dataKey,
        type: "put",
        value: JSON.stringify(request),
      });
      userList.push({
        key: userKey,
        type: "put",
        value: dataKey,
      });
      appList.push({
        key: appKey,
        type: "put",
        value: dataKey,
      });
    }
    console.log(`Inserting batch #${index}`);
    await history.batch(singleList);
    await historyData.batch(dataList);
    await historyUser.batch(userList);
    await historyApp.batch(appList);
  }

  generateRequest() {
    const { mock } = this;
    const url = mock.internet.uri();
    const method = mock.internet.httpMethod();
    const headers = mock.http.headers.headers("request");
    const payload = mock.http.payload.payload("application/json");
    return {
      url,
      method,
      headers,
      payload,
    };
  }

  /**
   * Queries for all data and computes speed of read.
   * @private
   */
  async measureByTime() {
    const cond = true;
    let page = 1;
    let lastKey = "";

    // traditional, keys filtering
    const timeTradKeysMark = `time-trad-keys`;
    performance.mark(timeTradKeysMark);
    while (cond) {
      const mark = `${timeTradKeysMark}-p-${page}`;
      performance.mark(mark);
      const result = await this.listTraditionalKeys({
        lastKey,
        limit: 250,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`${timeTradKeysMark}-all`, timeTradKeysMark);

    // traditional, values filtering
    page = 1;
    lastKey = "";
    const timeTradValuesMark = `time-trad-values`;
    performance.mark(timeTradValuesMark);
    while (cond) {
      const mark = `${timeTradValuesMark}-p-${page}`;
      performance.mark(mark);
      const result = await this.listTraditionalValues({
        lastKey,
        limit: 250,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`${timeTradValuesMark}-all`, timeTradValuesMark);

    // keys filtering
    page = 1;
    lastKey = "";
    const timeKeyFilterMark = `time-key-filter`;
    performance.mark(timeKeyFilterMark);
    while (cond) {
      const mark = `${timeKeyFilterMark}-p-${page}`;
      performance.mark(mark);
      const result = await this.listKeyBased({
        lastKey,
        limit: 250,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`${timeKeyFilterMark}-all`, timeKeyFilterMark);

    // index tables
    page = 1;
    lastKey = "";
    const indexFilterMark = `time-index`;
    performance.mark(indexFilterMark);
    while (cond) {
      const mark = `${indexFilterMark}-p-${page}`;
      performance.mark(mark);
      const result = await this.listIndexTable({
        lastKey,
        limit: 250,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.data.length) {
        break;
      }
      lastKey = result.lastKey;
      page++;
    }
    performance.measure(`${indexFilterMark}-all`, indexFilterMark);
  }

  /**
   * Queries for all data and computes speed of read.
   * @param {string} app
   * @private
   */
  async measureByApp(app) {
    const cond = true;
    let page = 1;
    let lastKey = "";

    // traditional, keys filtering
    const timeTradKeysMark = `app-time-trad-keys`;
    performance.mark(timeTradKeysMark);
    while (cond) {
      const mark = `app-time-trad-keys-p-${page}`;
      performance.mark(mark);
      const result = await this.listTraditionalKeys({
        lastKey,
        limit: 250,
        app,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`app-time-trad-keys-all`, timeTradKeysMark);

    // traditional, values filtering
    page = 1;
    lastKey = "";
    const timeTradValuesMark = `app-time-trad-values`;
    performance.mark(timeTradValuesMark);
    while (cond) {
      const mark = `app-time-trad-values-p-${page}`;
      performance.mark(mark);
      const result = await this.listTraditionalValues({
        lastKey,
        limit: 250,
        app,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`app-time-trad-values-all`, timeTradValuesMark);

    // keys filtering
    page = 1;
    lastKey = "";
    const timeKeyFilterMark = `app-time-key-filter`;
    performance.mark(timeKeyFilterMark);
    while (cond) {
      const mark = `app-time-key-filter-p-${page}`;
      performance.mark(mark);
      const result = await this.listKeyBased({
        lastKey,
        limit: 250,
        app,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`app-time-key-filter-all`, timeKeyFilterMark);

    // index tables
    page = 1;
    let lastAppKey = "";
    const indexFilterMark = `app-time-index`;
    performance.mark(indexFilterMark);
    while (cond) {
      const mark = `${indexFilterMark}-p-${page}`;
      performance.mark(mark);
      const result = await this.listIndexTable({
        lastAppKey,
        limit: 250,
        app,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.data.length) {
        break;
      }
      lastAppKey = result.lastAppKey;
      page++;
    }
    performance.measure(`${indexFilterMark}-all`, indexFilterMark);
  }

  /**
   * Queries for all data and computes speed of read.
   * @param {string} user
   * @private
   */
  async measureByUser(user) {
    const cond = true;
    let page = 1;
    let lastKey = "";

    // traditional, keys filtering
    const timeTradKeysMark = `user-time-trad-keys`;
    performance.mark(timeTradKeysMark);
    while (cond) {
      const mark = `user-time-trad-keys-p-${page}`;
      performance.mark(mark);
      const result = await this.listTraditionalKeys({
        lastKey,
        limit: 250,
        user,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`user-time-trad-keys-all`, timeTradKeysMark);

    // traditional, values filtering
    page = 1;
    lastKey = "";
    const timeTradValuesMark = `user-time-trad-values`;
    performance.mark(timeTradValuesMark);
    while (cond) {
      const mark = `user-time-trad-values-p-${page}`;
      performance.mark(mark);
      const result = await this.listTraditionalValues({
        lastKey,
        limit: 250,
        user,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`user-time-trad-values-all`, timeTradValuesMark);

    // keys filtering
    page = 1;
    lastKey = "";
    const timeKeyFilterMark = `user-time-key-filter`;
    performance.mark(timeKeyFilterMark);
    while (cond) {
      const mark = `user-time-key-filter-p-${page}`;
      performance.mark(mark);
      const result = await this.listKeyBased({
        lastKey,
        limit: 250,
        user,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.length) {
        break;
      }
      lastKey = result[result.length - 1].key;
      page++;
    }
    performance.measure(`user-time-key-filter-all`, timeKeyFilterMark);

    // index tables
    page = 1;
    let lastUserKey = "";
    const indexFilterMark = `user-time-index`;
    performance.mark(indexFilterMark);
    while (cond) {
      const mark = `${indexFilterMark}-p-${page}`;
      performance.mark(mark);
      const result = await this.listIndexTable({
        lastUserKey,
        limit: 250,
        user,
      });
      performance.measure(`${mark}-time`, mark);
      if (!result.data.length) {
        break;
      }
      lastUserKey = result.lastUserKey;
      page++;
    }
    performance.measure(`${indexFilterMark}-all`, indexFilterMark);
  }

  /**
   * Uses the compound key to filter out the data.
   * Values are always read from the store but parsed only when filter passes with success.
   *
   * - always reads keys
   * - always reads values
   * - only parses values when needed to returns
   *
   * @param {QueryOptions=} opts
   */
  async listTraditionalKeys(opts = {}) {
    const { history } = this;
    const itOpts = /** @type AbstractIteratorOptions */ ({
      reverse: true,
    });
    const iterator = history.iterator(itOpts);
    if (opts.lastKey) {
      iterator.seek(opts.lastKey);
      // @ts-ignore
      await iterator.next();
    }
    const data = [];
    const { limit = 35, app, user } = opts;
    const hasQuery = !!app || !!user;
    let remaining = limit;
    // @ts-ignore
    for await (const [key, value] of iterator) {
      if (hasQuery) {
        const parts = key.split("~"); // [time, app, user]
        if (app) {
          if (parts[1] !== app) {
            continue;
          }
        }
        if (user) {
          if (parts[2] !== user) {
            continue;
          }
        }
      }
      data.push(JSON.parse(value));
      remaining -= 1;
      if (remaining === 0) {
        break;
      }
    }
    return data;
  }

  /**
   * Ignores the key and compares data from the parsed value.
   *
   * - never reads keys
   * - always reads values
   * - always parses values
   *
   * @param {QueryOptions=} opts
   */
  async listTraditionalValues(opts = {}) {
    const { history } = this;
    const itOpts = /** @type AbstractIteratorOptions */ ({
      reverse: true,
      keys: false,
    });
    const iterator = history.iterator(itOpts);
    if (opts.lastKey) {
      iterator.seek(opts.lastKey);
      // @ts-ignore
      await iterator.next();
    }
    const data = [];
    const { limit = 35, app, user } = opts;
    const hasQuery = !!app || !!user;
    let remaining = limit;
    // @ts-ignore
    for await (const [, value] of iterator) {
      const parsed = JSON.parse(value);
      if (hasQuery) {
        if (app) {
          if (parsed.app !== app) {
            continue;
          }
        }
        if (user) {
          if (parsed.user !== user) {
            continue;
          }
        }
      }
      data.push(parsed);
      remaining -= 1;
      if (remaining === 0) {
        break;
      }
    }
    return data;
  }

  /**
   * It iterates over keys only and performs filtering.
   * After that it batch-queries for the data and parses them
   *
   * - always reads keys
   * - reads values only when needed
   * - parses values for final result only.
   * @param {QueryOptions=} opts
   */
  async listKeyBased(opts = {}) {
    const { history } = this;
    const itOpts = /** @type AbstractIteratorOptions */ ({
      reverse: true,
      values: false,
    });
    const iterator = history.iterator(itOpts);
    if (opts.lastKey) {
      iterator.seek(opts.lastKey);
      // @ts-ignore
      await iterator.next();
    }
    const { limit = 35, app, user } = opts;
    const hasQuery = !!app || !!user;
    const keys = /** @type string[] */ ([]);
    let remaining = limit;
    // @ts-ignore
    for await (const [key] of iterator) {
      if (hasQuery) {
        const parts = key.split("~"); // [time, app, user]
        if (app) {
          if (parts[1] !== app) {
            continue;
          }
        }
        if (user) {
          if (parts[2] !== user) {
            continue;
          }
        }
      }
      keys.push(key);
      remaining -= 1;
      if (remaining === 0) {
        break;
      }
    }
    if (!keys.length) {
      return [];
    }
    const result = await history.getMany(keys);
    return result.map((item) => JSON.parse(item));
  }

  /**
   * A different kind of querying where the data is in one table,
   * the user key in another table references the data records, and the same for the app.
   * 
   * Essentially we have 3 tables
   *  
   * 1. keeps the history data. Keys are ordered by the time so can perform "all" query.
   * 2. keeps the user data. The key is the user UID + created time as the key for ordered queries. The value is the id of a data entry.
   * 3. keeps the app data. The key is the app UID + created time as the key for ordered queries. The value is the id of a data entry.
   * 
   * @param {IndexQueryOptions=} opts
   * @returns {Promise<IndexQueryResult>}
   */
  async listIndexTable(opts = {}) {
    const { historyData } = this;
    const hasQuery = !!opts.app || !!opts.user;
    const result = /** @type IndexQueryResult */ ({
      data: [],
    });
    if (hasQuery) {
      let ids = /** @type string[] */ ([]);
      if (opts.app) {
        const info = await this.listAppHistoryKeys(opts);
        if (info.keys.length) {
          result.lastAppKey = info.lastKey;
          ids = ids.concat(info.keys);
        }
      } else if (opts.user) {
        const info = await this.listUserHistoryKeys(opts);
        if (info.keys.length) {
          result.lastUserKey = info.lastKey;
          ids = ids.concat(info.keys);
        }
      }
      const data = await historyData.getMany([...new Set(ids)]);
      result.data = data.map((item) => JSON.parse(item));
      return result;
    }

    // otherwise iterate over the data table
    const itOpts = /** @type AbstractIteratorOptions */ ({
      reverse: true,
    });
    const iterator = historyData.iterator(itOpts);
    if (opts.lastKey) {
      iterator.seek(opts.lastKey);
      // @ts-ignore
      await iterator.next();
    }
    const { limit = 35 } = opts;
    let remaining = limit;
    let lastKey = undefined;
    // @ts-ignore
    for await (const [key, value] of iterator) {
      result.data.push(JSON.parse(value));
      lastKey = key;
      remaining -= 1;
      if (remaining === 0) {
        break;
      }
    }
    result.lastKey = lastKey;
    return result;
  }

  /**
   * Lists data keys for an app query
   * @param {IndexQueryOptions} opts 
   * @returns {Promise<IndexKeysResult>}
   */
  async listAppHistoryKeys(opts) {
    const { historyApp } = this;
    const { limit = 35, app } = opts;
    const itOpts = /** @type AbstractIteratorOptions */ ({
      reverse: true,
      gte: `~${app}~`,
      lte: `~${app}~~`,
    });
    const iterator = historyApp.iterator(itOpts);
    if (opts.lastAppKey) {
      iterator.seek(opts.lastAppKey);
      // @ts-ignore
      await iterator.next();
    }
    const keys = /** @type string[] */ ([]);
    let remaining = limit;
    let lastKey = undefined;
    // @ts-ignore
    for await (const [key, value] of iterator) {
      keys.push(value.toString());
      lastKey = key;
      remaining -= 1;
      if (remaining === 0) {
        break;
      }
    }
    return {
      keys,
      lastKey,
    };
  }

  /**
   * Lists data keys for a user query
   * @param {IndexQueryOptions} opts 
   * @returns {Promise<IndexKeysResult>}
   */
  async listUserHistoryKeys(opts = {}) {
    const { historyUser } = this;
    const { limit = 35, user } = opts;
    const itOpts = /** @type AbstractIteratorOptions */ ({
      reverse: true,
      gte: `~${user}~`,
      lte: `~${user}~~`,
    });
    const iterator = historyUser.iterator(itOpts);
    if (opts.lastUserKey) {
      iterator.seek(opts.lastUserKey);
      // @ts-ignore
      await iterator.next();
    }
    
    const keys = /** @type string[] */ ([]);
    let remaining = limit;
    let lastKey = undefined;
    // @ts-ignore
    for await (const [key, value] of iterator) {
      keys.push(value.toString());
      lastKey = key;
      remaining -= 1;
      if (remaining === 0) {
        break;
      }
    }
    return {
      keys,
      lastKey,
    };
  }
}

const instance = new TestHistory();
instance.start();
