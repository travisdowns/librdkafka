/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Apache Kafka consumer & producer performance tester
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <vector>
#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS /* Silence nonsense on MSVC */
#endif

#include "../src/rd.h"

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */
/* Do not include these defines from your program, they will not be
 * provided by librdkafka. */
#include "rd.h"
#include "rdtime.h"

#ifdef _WIN32
#include "../win32/wingetopt.h"
#include "../win32/wintime.h"
#endif


static volatile sig_atomic_t run = 1;
static int forever               = 1;
static rd_ts_t dispintvl         = 1000;
static int do_seq                = 0;
static int exit_after            = 0;
static int exit_eof              = 0;
static FILE *stats_fp;
static int verbosity        = 1;
static int latency_mode     = 0;
static FILE *latency_fp     = NULL;
static int msgcnt           = -1;
static int incremental_mode = 0;
static int partition_cnt    = 0;
static int eof_cnt          = 0;
static int with_dr          = 1;
static int read_hdrs        = 0;


static void stop(int sig) {
        if (!run) {
                fprintf(stderr, "Second SIGINT, force exit()");
                exit(0);
        }
        fprintf(stderr, "-- SIGINT: Stop requested --");
        run = 0;
}

struct avg {
        int64_t val;
        int cnt;
        uint64_t ts_start;
};

struct cnt_t {
        rd_ts_t t_start;
        rd_ts_t t_end;
        rd_ts_t t_end_send;
        uint64_t msgs;
        uint64_t msgs_last;
        uint64_t msgs_dr_ok;
        uint64_t msgs_dr_err;
        uint64_t eof_cnt;
        uint64_t bytes_dr_ok;
        uint64_t bytes;
        uint64_t bytes_last;
        uint64_t tx;
        uint64_t tx_err;
        uint64_t avg_rtt;
        uint64_t offset;
        uint64_t fetch_count;
        rd_ts_t t_fetch_latency;
        rd_ts_t t_last;
        rd_ts_t t_enobufs_last;
        rd_ts_t t_total;
        rd_ts_t latency_last;
        rd_ts_t latency_lo;
        rd_ts_t latency_hi;
        rd_ts_t latency_sum;
        int latency_cnt;
        int64_t last_offset;
};


uint64_t wall_clock(void) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return ((uint64_t)tv.tv_sec * 1000000LLU) + ((uint64_t)tv.tv_usec);
}

static void err_cb(rd_kafka_t *rk, int err_, const char *reason, void *opaque) {
        auto err = (rd_kafka_resp_err_t)err_;
        if (err == RD_KAFKA_RESP_ERR__FATAL) {
                char errstr[512];
                err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
                printf("%% FATAL ERROR CALLBACK: %s: %s: %s\n",
                       rd_kafka_name(rk), rd_kafka_err2str(err), errstr);
        } else {
                printf("%% ERROR CALLBACK: %s: %s: %s\n", rd_kafka_name(rk),
                       rd_kafka_err2str(err), reason);
        }
}

static void throttle_cb(rd_kafka_t *rk,
                        const char *broker_name,
                        int32_t broker_id,
                        int throttle_time_ms,
                        void *opaque) {
        printf("%% THROTTLED %dms by %s (%" PRId32 ")\n", throttle_time_ms,
               broker_name, broker_id);
}

static void offset_commit_cb(rd_kafka_t *rk,
                             rd_kafka_resp_err_t err,
                             rd_kafka_topic_partition_list_t *offsets,
                             void *opaque) {
        int i;

        if (err || verbosity >= 2)
                printf("%% Offset commit of %d partition(s): %s\n",
                       offsets->cnt, rd_kafka_err2str(err));

        for (i = 0; i < offsets->cnt; i++) {
                rd_kafka_topic_partition_t *rktpar = &offsets->elems[i];
                if (rktpar->err || verbosity >= 2)
                        printf("%%  %s [%" PRId32 "] @ %" PRId64 ": %s\n",
                               rktpar->topic, rktpar->partition, rktpar->offset,
                               rd_kafka_err2str(err));
        }
}


struct consumer_state {
        int cid;
        rd_kafka_t *rk;
        cnt_t cnt = {};
};

static void msg_consume(rd_kafka_message_t *rkmessage, consumer_state& consumer) {

        auto& cnt = consumer.cnt;

        if (rkmessage->err) {
                if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        cnt.offset = rkmessage->offset;

                        if (verbosity >= 2)
                                printf(
                                    "%% Consumer %5d reached end of "
                                    "%s [%" PRId32
                                    "] "
                                    "message queue at offset %" PRId64 "\n",
                                    consumer.cid,
                                    rd_kafka_topic_name(rkmessage->rkt),
                                    rkmessage->partition, rkmessage->offset);

                        if (exit_eof && ++eof_cnt == partition_cnt)
                                run = 0;

                        cnt.eof_cnt++;

                        return;
                }

                printf("%% Consume error for topic \"%s\" [%" PRId32
                       "] "
                       "offset %" PRId64 ": %s\n",
                       rkmessage->rkt ? rd_kafka_topic_name(rkmessage->rkt)
                                      : "",
                       rkmessage->partition, rkmessage->offset,
                       rd_kafka_message_errstr(rkmessage));

                if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                    rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
                        run = 0;

                cnt.msgs_dr_err++;
                return;
        }

        /* Start measuring from first message received */
        if (!cnt.t_start)
                cnt.t_start = cnt.t_last = rd_clock();

        cnt.offset = rkmessage->offset;
        cnt.msgs++;
        cnt.bytes += rkmessage->len;

        if (verbosity >= 3 || (verbosity >= 2 && !(cnt.msgs % 1000000)))
                printf("@%" PRId64 ": %.*s: %.*s\n", rkmessage->offset,
                       (int)rkmessage->key_len, (char *)rkmessage->key,
                       (int)rkmessage->len, (char *)rkmessage->payload);

        if (read_hdrs) {
                rd_kafka_headers_t *hdrs;
                /* Force parsing of headers but don't do anything with them. */
                rd_kafka_message_headers(rkmessage, &hdrs);
        }

        if (msgcnt != -1 && (int)cnt.msgs >= msgcnt)
                run = 0;
}


static void rebalance_cb(rd_kafka_t *rk,
                         rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *partitions,
                         void *opaque) {
        rd_kafka_error_t *error     = NULL;
        rd_kafka_resp_err_t ret_err = RD_KAFKA_RESP_ERR_NO_ERROR;
        const consumer_state& consumer = *static_cast<const consumer_state*>(opaque);

        if (exit_eof && !strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE"))
                fprintf(stderr,
                        "%% This example has not been modified to "
                        "support -e (exit on EOF) when "
                        "partition.assignment.strategy "
                        "is set to an incremental/cooperative strategy: "
                        "-e will not behave as expected\n");

        switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                fprintf(stderr,
                        "%% Consumer %5d: group rebalanced (%s): "
                        "%d new partition(s) assigned\n",
                        consumer.cid,
                        rd_kafka_rebalance_protocol(rk), partitions->cnt);

                if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE")) {
                        error = rd_kafka_incremental_assign(rk, partitions);
                } else {
                        ret_err = rd_kafka_assign(rk, partitions);
                        eof_cnt = 0;
                }

                partition_cnt += partitions->cnt;
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                fprintf(stderr,
                        "%% Group rebalanced (%s): %d partition(s) revoked, rkptr %p\n",
                        rd_kafka_rebalance_protocol(rk), partitions->cnt, rk);

                if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE")) {
                        error = rd_kafka_incremental_unassign(rk, partitions);
                        partition_cnt -= partitions->cnt;
                } else {
                        ret_err       = rd_kafka_assign(rk, NULL);
                        partition_cnt = 0;
                }

                eof_cnt = 0; /* FIXME: Not correct for incremental case */
                break;

        default:
                break;
        }

        if (error) {
                fprintf(stderr, "%% incremental assign failure: %s\n",
                        rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
        } else if (ret_err) {
                fprintf(stderr, "%% assign failure: %s\n",
                        rd_kafka_err2str(ret_err));
        }
}


/**
 * Find and extract single value from a two-level search.
 * First find 'field1', then find 'field2' and extract its value.
 * Returns 0 on miss else the value.
 */
static uint64_t json_parse_fields(const char *json,
                                  const char **end,
                                  const char *field1,
                                  const char *field2) {
        const char *t = json;
        const char *t2;
        int len1 = (int)strlen(field1);
        int len2 = (int)strlen(field2);

        while ((t2 = strstr(t, field1))) {
                uint64_t v;

                t = t2;
                t += len1;

                /* Find field */
                if (!(t2 = strstr(t, field2)))
                        continue;
                t2 += len2;

                while (isspace((int)*t2))
                        t2++;

                v = strtoull(t2, (char **)&t, 10);
                if (t2 == t)
                        continue;

                *end = t;
                return v;
        }

        *end = t + strlen(t);
        return 0;
}

/**
 * Parse various values from rdkafka stats
 */
static void json_parse_stats(const char *json, consumer_state& consumer) {
        const char *t;
#define MAX_AVGS 100 /* max number of brokers to scan for rtt */
        uint64_t avg_rtt[MAX_AVGS + 1];
        int avg_rtt_i = 0;

        /* Store totals at end of array */
        avg_rtt[MAX_AVGS] = 0;

        /* Extract all broker RTTs */
        t = json;
        while (avg_rtt_i < MAX_AVGS && *t) {
                avg_rtt[avg_rtt_i] =
                    json_parse_fields(t, &t, "\"rtt\":", "\"avg\":");

                /* Skip low RTT values, means no messages are passing */
                if (avg_rtt[avg_rtt_i] < 100 /*0.1ms*/)
                        continue;


                avg_rtt[MAX_AVGS] += avg_rtt[avg_rtt_i];
                avg_rtt_i++;
        }

        if (avg_rtt_i > 0)
                avg_rtt[MAX_AVGS] /= avg_rtt_i;

        consumer.cnt.avg_rtt = avg_rtt[MAX_AVGS];
}


static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {

        /* Extract values for our own stats */
        json_parse_stats(json, *static_cast<consumer_state*>(opaque));

        if (stats_fp)
                fprintf(stats_fp, "%s\n", json);
        return 0;
}

#define _OTYPE_TAB     0x1 /* tabular format */
#define _OTYPE_SUMMARY 0x2 /* summary format */
#define _OTYPE_FORCE   0x4 /* force output regardless of interval timing */

static void
print_stats(consumer_state& consumer, int otype, const char *compression) {
        auto& cnt = consumer.cnt;
        rd_ts_t now = rd_clock();
        rd_ts_t t_total;
        static int rows_written = 0;
        int print_header;
        double latency_avg = 0.0f;
        char extra[512];
        int extra_of = 0;
        *extra       = '\0';

        if (!(otype & _OTYPE_FORCE) &&
            (((otype & _OTYPE_SUMMARY) && verbosity == 0) ||
             cnt.t_last + dispintvl > now))
                return;

        print_header = !rows_written || (verbosity > 0 && !(rows_written % 20));

        if (cnt.t_end_send)
                t_total = cnt.t_end_send - cnt.t_start;
        else if (cnt.t_end)
                t_total = cnt.t_end - cnt.t_start;
        else if (cnt.t_start)
                t_total = now - cnt.t_start;
        else
                t_total = 1;

        if (latency_mode && cnt.latency_cnt)
                latency_avg = (double)cnt.latency_sum / (double)cnt.latency_cnt;


#define ROW_START()                                                            \
        do {                                                                   \
        } while (0)
#define COL_HDR(NAME)       printf("| %10.10s ", (NAME))
#define COL_PR64(NAME, VAL) printf("| %10" PRIu64 " ", (VAL))
#define COL_PRF(NAME, VAL)  printf("| %10.2f ", (VAL))
#define ROW_END()                                                              \
        do {                                                                   \
                printf("\n");                                                  \
                rows_written++;                                                \
        } while (0)


        if (otype & _OTYPE_TAB) {
                if (print_header) {
                        /* First time, print header */
                        ROW_START();
                        COL_HDR("elapsed");
                        COL_HDR("msgs");
                        COL_HDR("bytes");
                        COL_HDR("rtt");
                        COL_HDR("m/s");
                        COL_HDR("MB/s");
                        COL_HDR("rx_err");
                        COL_HDR("offset");
                        if (latency_mode) {
                                COL_HDR("lat_curr");
                                COL_HDR("lat_avg");
                                COL_HDR("lat_lo");
                                COL_HDR("lat_hi");
                        }
                        ROW_END();
                }

                ROW_START();
                COL_PR64("elapsed", t_total / 1000);
                COL_PR64("msgs", cnt.msgs);
                COL_PR64("bytes", cnt.bytes);
                COL_PR64("rtt", cnt.avg_rtt / 1000);
                COL_PR64("m/s", ((cnt.msgs * 1000000) / t_total));
                COL_PRF("MB/s", (float)((cnt.bytes) / (float)t_total));
                COL_PR64("rx_err", cnt.msgs_dr_err);
                COL_PR64("offset", cnt.offset);
                if (latency_mode) {
                        COL_PRF("lat_curr", cnt.latency_last / 1000.0f);
                        COL_PRF("lat_avg", latency_avg / 1000.0f);
                        COL_PRF("lat_lo", cnt.latency_lo / 1000.0f);
                        COL_PRF("lat_hi", cnt.latency_hi / 1000.0f);
                }
                ROW_END();
        }

        if (otype & _OTYPE_SUMMARY) {
                if (latency_avg >= 1.0f)
                        extra_of += rd_snprintf(
                                extra + extra_of, sizeof(extra) - extra_of,
                                ", latency "
                                "curr/avg/lo/hi "
                                "%.2f/%.2f/%.2f/%.2fms",
                                cnt.latency_last / 1000.0f,
                                latency_avg / 1000.0f,
                                cnt.latency_lo / 1000.0f,
                                cnt.latency_hi / 1000.0f);
                printf("%% Consumer %5d %" PRIu64 " messages (%" PRIu64
                        " bytes) "
                        "consumed in %" PRIu64 "ms: %" PRIu64
                        " msgs/s "
                        "(%.02f MB/s)"
                        "%s\n",
                        consumer.cid,
                        cnt.msgs, cnt.bytes, t_total / 1000,
                        ((cnt.msgs * 1000000) / t_total),
                        (float)((cnt.bytes) / (float)t_total), extra);
        }

        if (incremental_mode && now > cnt.t_last) {
                uint64_t i_msgs  = cnt.msgs - cnt.msgs_last;
                uint64_t i_bytes = cnt.bytes - cnt.bytes_last;
                uint64_t i_time  = cnt.t_last ? now - cnt.t_last : 0;

                printf("%% INTERVAL: %" PRIu64
                        " messages "
                        "(%" PRIu64
                        " bytes) "
                        "consumed in %" PRIu64 "ms: %" PRIu64
                        " msgs/s "
                        "(%.02f MB/s)"
                        "%s\n",
                        i_msgs, i_bytes, i_time / 1000,
                        ((i_msgs * 1000000) / i_time),
                        (float)((i_bytes) / (float)i_time), extra);
        }

        cnt.t_last     = now;
        cnt.msgs_last  = cnt.msgs;
        cnt.bytes_last = cnt.bytes;
}

template <typename Fn>
auto aggr(std::vector<consumer_state>& consumers, Fn f) {
        auto sum = f(consumers[0]);
        for (size_t i = 1; i < consumers.size(); i++) {
                sum += f(consumers[i]);
        }
        return sum;
}

void print_aggr_stats(std::vector<consumer_state>& consumers, uint64_t run_start) {

        uint64_t elapsed = rd_clock() - run_start;

#define PRINT1(format, field) { \
                auto r = aggr(consumers, [](auto& c) { return c.cnt.field; }); \
                printf(format, r); \
        }

#define PRINTE(format, field) { \
                auto r = aggr(consumers, [](auto& c) { return c.cnt.field; }); \
                printf(format, r * 1000000. / elapsed ); \
        }

        PRINT1("fetches %zu ", fetch_count);
        PRINTE("fetch/s %5.2f ", fetch_count);
        PRINT1("msgs %zu ", msgs);
        PRINT1("eof %zu ", eof_cnt);

        printf("\n");
}


static void sig_usr1(int sig) {
        printf("sig_usr1 detected");
        // rd_kafka_dump(stdout, global_rk);
}


/**
 * @brief Read config from file
 * @returns -1 on error, else 0.
 */
static int read_conf_file(rd_kafka_conf_t *conf, const char *path) {
        FILE *fp;
        char buf[512];
        int line = 0;
        char errstr[512];

        if (!(fp = fopen(path, "r"))) {
                fprintf(stderr, "%% Failed to open %s: %s\n", path,
                        strerror(errno));
                return -1;
        }

        while (fgets(buf, sizeof(buf), fp)) {
                char *s = buf;
                char *t;
                rd_kafka_conf_res_t r = RD_KAFKA_CONF_UNKNOWN;

                line++;

                while (isspace((int)*s))
                        s++;

                if (!*s || *s == '#')
                        continue;

                if ((t = strchr(buf, '\n')))
                        *t = '\0';

                t = strchr(buf, '=');
                if (!t || t == s || !*(t + 1)) {
                        fprintf(stderr, "%% %s:%d: expected key=value\n", path,
                                line);
                        fclose(fp);
                        return -1;
                }

                *(t++) = '\0';

                /* Try global config */
                r = rd_kafka_conf_set(conf, s, t, errstr, sizeof(errstr));

                if (r == RD_KAFKA_CONF_OK)
                        continue;

                fprintf(stderr, "%% %s:%d: %s=%s: %s\n", path, line, s, t,
                        errstr);
                fclose(fp);
                return -1;
        }

        fclose(fp);

        return 0;
}


/**
 * @brief Sleep for \p sleep_us microseconds.
 */
static void do_sleep(int sleep_us) {
        if (sleep_us > 100) {
#ifdef _WIN32
                Sleep(sleep_us / 1000);
#else
                usleep(sleep_us);
#endif
        } else {
                rd_ts_t next = rd_clock() + (rd_ts_t)sleep_us;
                while (next > rd_clock())
                        ;
        }
}

struct consumer_args {
        const rd_kafka_conf_t* conf;
        const rd_kafka_topic_partition_list_t *topics;
        const char *compression;
        int rate_sleep;
        int otype = _OTYPE_SUMMARY;
};

void run_consumers(const consumer_args& args, int count) {

        char errstr[512];

        std::vector<consumer_state> consumers;
        consumers.reserve(count);

        fprintf(stderr, "Setting up %d consumers\n", count);
        for (int cid = 0; cid < count; cid++) {

                consumers.push_back({cid, nullptr});

                rd_kafka_t *rk;

                auto conf = rd_kafka_conf_dup(args.conf);
                rd_kafka_conf_set_opaque(conf, &consumers.back());

                /* Create Kafka handle */
                if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr,
                                        sizeof(errstr)))) {
                        fprintf(stderr,
                                "%% Failed to create Kafka consumer: %s\n",
                                errstr);
                        exit(1);
                }

                consumers.back().rk = rk;
        }

        assert(rd_kafka_opaque(consumers.front().rk) == &consumers.front());
        assert(rd_kafka_opaque(consumers.back().rk) == &consumers.back());

        for (auto& consumer : consumers) {
                /* Forward all events to consumer queue */
                rd_kafka_poll_set_consumer(consumer.rk);

                auto err = rd_kafka_subscribe(consumer.rk, args.topics);

                if (err) {
                        fprintf(stderr, "%% Subscribe failed: %s\n",
                                rd_kafka_err2str(err));
                        exit(1);
                }
        }

        fprintf(stderr, "Subscribe complete\n");

        fprintf(stderr, "%% Waiting for group rebalance..\n");

        uint64_t run_start = rd_clock();

        while (run && (msgcnt == -1 || msgcnt > (int)consumers.front().cnt.msgs)) {

                for (auto& consumer : consumers) {
                        /* Consume messages.
                        * A message may either be a real message, or
                        * an event (if rkmessage->err is set).
                        */
                        rd_kafka_message_t *rkmessage;
                        uint64_t fetch_latency = rd_clock();

                        rkmessage = rd_kafka_consumer_poll(consumer.rk, 0);
                        if (rkmessage) {
                                msg_consume(rkmessage, consumer);
                                rd_kafka_message_destroy(rkmessage);

                                /* Simulate processing time
                                * if `-r <rate>` was set. */
                                if (args.rate_sleep)
                                        do_sleep(args.rate_sleep);
                        } else {
                                // throttle a bit if we aren't getting messages
                                do_sleep(100 * 1000);
                        }

                        //  else {
                        //         fprintf(stderr, "%% Consumer %5d no message\n", consumer.cid);
                        // }
                        consumer.cnt.fetch_count++;
                        consumer.cnt.t_fetch_latency += rd_clock() - fetch_latency;
                        // print_stats(consumer, args.otype, args.compression);
                }

                // do_sleep(10000000);
                print_aggr_stats(consumers, run_start);

        }

        auto t_end = rd_clock();

        for (auto& consumer : consumers) {
                consumer.cnt.t_end = t_end;
                print_stats(consumer, args.otype | _OTYPE_FORCE, args.compression);

                auto err = rd_kafka_consumer_close(consumer.rk);
                if (err)
                        fprintf(stderr, "%% Failed to close consumer: %s\n",
                                rd_kafka_err2str(err));
        }

        for (auto& consumer : consumers) {
                rd_kafka_destroy(consumer.rk);
                consumer.rk = nullptr;
        }

}

void conf_set_and_check(rd_kafka_conf_t *conf,
                        const char *name,
                        const char *value) {

        auto res =
            rd_kafka_conf_set(conf, name, value, NULL, 0);
        if (res != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "ERROR: counld not set %s to %s", name, value);
                exit(1);
        }
}

int main(int argc, char **argv) {
        char *brokers   = NULL;
        int *partitions = NULL;
        int opt;
        int sendflags          = 0;
        const char *msgpattern = "librdkafka_performance testing!";
        int msgsize            = -1;
        const char *debug      = NULL;
        int do_conf_dump       = 0;
        char errstr[512];
        int seed     = (int)time(NULL);
        int64_t start_offset    = 0;
        const char *stats_cmd   = NULL;
        char *stats_intvlstr    = NULL;
        char tmp[128];
        char *tmp2;
        double dtmp;
        int exitcode             = 0;
        rd_kafka_headers_t *hdrs = NULL;
        rd_kafka_resp_err_t err;

        consumer_args cargs{};

        /* Kafka configuration */
        auto conf = rd_kafka_conf_new();
        cargs.conf = conf;

        rd_kafka_conf_set_error_cb(conf, err_cb);
        rd_kafka_conf_set_throttle_cb(conf, throttle_cb);
        rd_kafka_conf_set_offset_commit_cb(conf, offset_commit_cb);

#ifdef SIGIO
        /* Quick termination */
        rd_snprintf(tmp, sizeof(tmp), "%i", SIGIO);
        rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);
#endif

        conf_set_and_check(conf, "auto.offset.reset", "smallest");
        conf_set_and_check(conf, "enable.auto.commit", "true");
        conf_set_and_check(conf, "enable.auto.offset.store", "false");
        conf_set_and_check(conf, "auto.commit.interval.ms", "5000");
        conf_set_and_check(conf, "partition.assignment.strategy", "roundrobin");

        auto topics = rd_kafka_topic_partition_list_new(1);
        cargs.topics = topics;

        int ccount = 1;

        while ((opt = getopt(argc, argv,
                             "G:t:p:b:s:c:fi:MDd:m:S:x:n:"
                             "R:a:z:o:X:B:eT:Y:qvIur:lA:OwNH:")) != -1) {
                switch (opt) {
                case 'G':
                        if (rd_kafka_conf_set(conf, "group.id", optarg, errstr,
                                              sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                exit(1);
                        }
                        /* FALLTHRU */
                        break;
                case 'n':  // consumer count
                        ccount = atoi(optarg);
                        break;
                case 't':
                        rd_kafka_topic_partition_list_add(
                            topics, optarg, RD_KAFKA_PARTITION_UA);
                        break;
                case 'p':
                        partition_cnt++;
                        partitions = (int *)realloc(
                            partitions, sizeof(*partitions) * partition_cnt);
                        partitions[partition_cnt - 1] = atoi(optarg);
                        break;

                case 'b':
                        brokers = optarg;
                        break;
                case 's':
                        msgsize = atoi(optarg);
                        break;
                case 'c':
                        msgcnt = atoi(optarg);
                        break;
                case 'D':
                        sendflags |= RD_KAFKA_MSG_F_FREE;
                        break;
                case 'i':
                        dispintvl = atoi(optarg);
                        break;
                case 'm':
                        msgpattern = optarg;
                        break;
                case 'x':
                        exit_after = atoi(optarg);
                        break;
                case 'R':
                        seed = atoi(optarg);
                        break;
                case 'a':
                        if (rd_kafka_conf_set(conf, "acks", optarg, errstr,
                                              sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                exit(1);
                        }
                        break;
                case 'z':
                        if (rd_kafka_conf_set(conf, "compression.codec", optarg,
                                              errstr, sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                exit(1);
                        }
                        cargs.compression = optarg;
                        break;
                case 'o':
                        if (!strcmp(optarg, "end"))
                                start_offset = RD_KAFKA_OFFSET_END;
                        else if (!strcmp(optarg, "beginning"))
                                start_offset = RD_KAFKA_OFFSET_BEGINNING;
                        else if (!strcmp(optarg, "stored"))
                                start_offset = RD_KAFKA_OFFSET_STORED;
                        else {
                                start_offset = strtoll(optarg, NULL, 10);

                                if (start_offset < 0)
                                        start_offset =
                                            RD_KAFKA_OFFSET_TAIL(-start_offset);
                        }

                        break;
                case 'e':
                        exit_eof = 1;
                        break;
                case 'd':
                        debug = optarg;
                        break;
                case 'H':
                        if (!strcmp(optarg, "parse"))
                                read_hdrs = 1;
                        else {
                                char *name, *val;
                                size_t name_sz = -1;

                                name = optarg;
                                val  = strchr(name, '=');
                                if (val) {
                                        name_sz = (size_t)(val - name);
                                        val++; /* past the '=' */
                                }

                                if (!hdrs)
                                        hdrs = rd_kafka_headers_new(8);

                                err = rd_kafka_header_add(hdrs, name, name_sz,
                                                          val, -1);
                                if (err) {
                                        fprintf(
                                            stderr,
                                            "%% Failed to add header %s: %s\n",
                                            name, rd_kafka_err2str(err));
                                        exit(1);
                                }
                        }
                        break;
                case 'X': {
                        char *name, *val;
                        rd_kafka_conf_res_t res;

                        if (!strcmp(optarg, "list") ||
                            !strcmp(optarg, "help")) {
                                rd_kafka_conf_properties_show(stdout);
                                exit(0);
                        }

                        if (!strcmp(optarg, "dump")) {
                                do_conf_dump = 1;
                                continue;
                        }

                        name = optarg;
                        if (!(val = strchr(name, '='))) {
                                fprintf(stderr,
                                        "%% Expected "
                                        "-X property=value, not %s\n",
                                        name);
                                exit(1);
                        }

                        *val = '\0';
                        val++;

                        if (!strcmp(name, "file")) {
                                if (read_conf_file(conf, val) == -1)
                                        exit(1);
                                break;
                        }

                        res = rd_kafka_conf_set(conf, name, val, errstr,
                                                sizeof(errstr));

                        if (res != RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                exit(1);
                        }
                } break;

                case 'T':
                        stats_intvlstr = optarg;
                        break;
                case 'Y':
                        stats_cmd = optarg;
                        break;

                case 'q':
                        verbosity--;
                        break;

                case 'v':
                        verbosity++;
                        break;

                case 'u':
                        cargs.otype = _OTYPE_TAB;
                        verbosity--; /* remove some fluff */
                        break;

                case 'r':
                        dtmp = strtod(optarg, &tmp2);
                        if (tmp2 == optarg ||
                            (dtmp >= -0.001 && dtmp <= 0.001)) {
                                fprintf(stderr, "%% Invalid rate: %s\n",
                                        optarg);
                                exit(1);
                        }

                        cargs.rate_sleep = (int)(1000000.0 / dtmp);
                        break;

                case 'l':
                        latency_mode = 1;
                        break;

                case 'A':
                        if (!(latency_fp = fopen(optarg, "w"))) {
                                fprintf(stderr, "%% Cant open %s: %s\n", optarg,
                                        strerror(errno));
                                exit(1);
                        }
                        break;

                case 'M':
                        incremental_mode = 1;
                        break;

                case 'N':
                        with_dr = 0;
                        break;

                default:
                        fprintf(stderr, "Unknown option: %c\n", opt);
                        goto usage;
                }
        }

        if (topics->cnt == 0 || optind != argc) {
                if (optind < argc)
                        fprintf(stderr, "Unknown argument: %s\n", argv[optind]);
        usage:
                fprintf(
                    stderr,
                    "Usage: %s [-C|-P] -t <topic> "
                    "[-p <partition>] [-b <broker,broker..>] [options..]\n"
                    "\n"
                    "librdkafka version %s (0x%08x)\n"
                    "\n"
                    " Options:\n"
                    "  -C | -P |    Consumer or Producer mode\n"
                    "  -G <groupid> High-level Kafka Consumer mode\n"
                    "  -t <topic>   Topic to consume / produce\n"
                    "  -p <num>     Partition (defaults to random). "
                    "Multiple partitions are allowed in -C consumer mode.\n"
                    "  -M           Print consumer interval stats\n"
                    "  -b <brokers> Broker address list (host[:port],..)\n"
                    "  -s <size>    Message size (producer)\n"
                    "  -k <key>     Message key (producer)\n"
                    "  -H <name[=value]> Add header to message (producer)\n"
                    "  -H parse     Read message headers (consumer)\n"
                    "  -c <cnt>     Messages to transmit/receive\n"
                    "  -x <cnt>     Hard exit after transmitting <cnt> "
                    "messages (producer)\n"
                    "  -D           Copy/Duplicate data buffer (producer)\n"
                    "  -i <ms>      Display interval\n"
                    "  -m <msg>     Message payload pattern\n"
                    "  -S <start>   Send a sequence number starting at "
                    "<start> as payload\n"
                    "  -R <seed>    Random seed value (defaults to time)\n"
                    "  -a <acks>    Required acks (producer): "
                    "-1, 0, 1, >1\n"
                    "  -B <size>    Consume batch size (# of msgs)\n"
                    "  -z <codec>   Enable compression:\n"
                    "               none|gzip|snappy\n"
                    "  -o <offset>  Start offset (consumer)\n"
                    "               beginning, end, NNNNN or -NNNNN\n"
                    "  -d [facs..]  Enable debugging contexts:\n"
                    "               %s\n"
                    "  -X <prop=name> Set arbitrary librdkafka "
                    "configuration property\n"
                    "  -X file=<path> Read config from file.\n"
                    "  -X list      Show full list of supported properties.\n"
                    "  -X dump      Show configuration\n"
                    "  -T <intvl>   Enable statistics from librdkafka at "
                    "specified interval (ms)\n"
                    "  -Y <command> Pipe statistics to <command>\n"
                    "  -I           Idle: dont produce any messages\n"
                    "  -q           Decrease verbosity\n"
                    "  -v           Increase verbosity (default 1)\n"
                    "  -u           Output stats in table format\n"
                    "  -r <rate>    Producer msg/s limit\n"
                    "  -l           Latency measurement.\n"
                    "               Needs two matching instances, one\n"
                    "               consumer and one producer, both\n"
                    "               running with the -l switch.\n"
                    "  -l           Producer: per-message latency stats\n"
                    "  -A <file>    Write per-message latency stats to "
                    "<file>. Requires -l\n"
                    "  -O           Report produced offset (producer)\n"
                    "  -N           No delivery reports (producer)\n"
                    "\n"
                    " In Consumer mode:\n"
                    "  consumes messages and prints thruput\n"
                    "  If -B <..> is supplied the batch consumer\n"
                    "  mode is used, else the callback mode is used.\n"
                    "\n"
                    " In Producer mode:\n"
                    "  writes messages of size -s <..> and prints thruput\n"
                    "\n",
                    argv[0], rd_kafka_version_str(), rd_kafka_version(),
                    RD_KAFKA_DEBUG_CONTEXTS);
                exit(1);
        }


        dispintvl *= 1000; /* us */

        if (verbosity > 1)
                printf("%% Using random seed %i, verbosity level %i\n", seed,
                       verbosity);
        srand(seed);
        signal(SIGINT, stop);
#ifdef SIGUSR1
        signal(SIGUSR1, sig_usr1);
#endif


        if (debug && rd_kafka_conf_set(conf, "debug", debug, errstr,
                                       sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                printf("%% Debug configuration failed: %s: %s\n", errstr,
                       debug);
                exit(1);
        }


        if (stats_intvlstr) {
                rd_kafka_conf_set_stats_cb(conf, stats_cb);
                /* if no user-desired stats, adjust stats interval
                 * to the display interval. */
                if (rd_kafka_conf_set(conf, "statistics.interval.ms",
                                stats_intvlstr, errstr,
                                sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                        fprintf(stderr, "%% %s\n", errstr);
                        exit(1);
                }
        }


        if (do_conf_dump) {
                const char **arr;
                size_t cnt;
                int pass;

                for (pass = 0; pass < 2; pass++) {
                        int i;

                        if (pass == 0) {
                                arr = rd_kafka_conf_dump(conf, &cnt);
                                printf("# Global config\n");
                        } else {
                                rd_kafka_topic_conf_t *topic_conf =
                                    rd_kafka_conf_get_default_topic_conf(conf);

                                if (topic_conf) {
                                        printf("# Topic config\n");
                                        arr = rd_kafka_topic_conf_dump(
                                            topic_conf, &cnt);
                                } else {
                                        arr = NULL;
                                }
                        }

                        if (!arr)
                                continue;

                        for (i = 0; i < (int)cnt; i += 2)
                                printf("%s = %s\n", arr[i], arr[i + 1]);

                        printf("\n");

                        rd_kafka_conf_dump_free(arr, cnt);
                }

                exit(0);
        }

        if (latency_mode)
                do_seq = 0;

        if (stats_intvlstr) {
                /* User enabled stats (-T) */

#ifndef _WIN32
                if (stats_cmd) {
                        if (!(stats_fp = popen(stats_cmd,
#ifdef __linux__
                                               "we"
#else
                                               "w"
#endif
                                               ))) {
                                fprintf(stderr,
                                        "%% Failed to start stats command: "
                                        "%s: %s",
                                        stats_cmd, strerror(errno));
                                exit(1);
                        }
                } else
#endif
                        stats_fp = stdout;
        }

        if (msgcnt != -1)
                forever = 0;

        if (msgsize == -1)
                msgsize = (int)strlen(msgpattern);

        // rd_kafka_conf_set(conf, "enable.partition.eof", "true", NULL, 0);

        /* Set bootstrap servers */
        if (brokers &&
            rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                exit(1);
        }

        /*
            * High-level balanced Consumer
            */

        rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

        run_consumers(cargs, ccount);

        if (hdrs)
                rd_kafka_headers_destroy(hdrs);

        // if (cnt.t_fetch_latency && cnt.msgs)
        //         printf("%% Average application fetch latency: %" PRIu64 "us\n",
        //                cnt.t_fetch_latency / cnt.msgs);

        if (latency_fp)
                fclose(latency_fp);

        if (stats_fp) {
#ifndef _WIN32
                pclose(stats_fp);
#endif
                stats_fp = NULL;
        }

        if (partitions)
                free(partitions);

        rd_kafka_topic_partition_list_destroy(topics);

        /* Let background threads clean up and terminate cleanly. */
        rd_kafka_wait_destroyed(2000);

        return exitcode;
}
