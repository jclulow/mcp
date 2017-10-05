
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <err.h>
#include <errno.h>
#include <ftw.h>
#include <thread.h>
#include <synch.h>
#include <atomic.h>

#include "./copyfile.h"
#include "./strset.h"

typedef struct copy_thread {
	thread_t ct_id;
	volatile uint64_t ct_progress;
} copy_thread_t;

volatile uint64_t g_starter_gun = 0;
volatile uint64_t g_error_stop = 0;
volatile uint64_t g_running;
unsigned g_nthreads = 32;
copy_thread_t *g_threads;

mutex_t g_files_lock = ERRORCHECKMUTEX;
strset_t *g_files = NULL;
strset_cursor_t *g_files_cursor = NULL;
uint64_t g_files_total_size = 0;

static const char *input = NULL;
static const char *output = NULL;
static int count = 0;

hrtime_t start;

static int
walk_tree_func(const char *path, const struct stat *st, int info,
    struct FTW *ftwp)
{
	if (strcmp(path, input) == 0) {
		/*
		 * XXX Skip the top-level directory...
		 */
		return (0);
	}

	if (strncmp(path, input, strlen(input)) != 0 ||
	    path[strlen(input)] != '/') {
		fprintf(stdout, "path out of prefix: \"%s\"\n", path);
		return (-1);
	}

	const char *x = path + strlen(input) + 1;

	if (info == FTW_F) {
		g_files_total_size += st->st_size;
		if (strset_add(g_files, x) != 0) {
			warn("could not add to file list");
			return (-1);
		}
		return (0);
	}

	if (info == FTW_D) {
		char *out;

		if (asprintf(&out, "%s/%s", output, x) < 0) {
			err(1, "asprintf");
		}

		/*
		 * XXX We'll make all of the directories up front, that way
		 * they will exist prior to the file copy phase.  The walk
		 * order of nftw(3C) ensures we should not hit any dependency
		 * issues.
		 */
		fprintf(stdout, "[%5d] mkdir \"%s\"\n", count++, out);
		if (mkdir(out, 0755) != 0) {
			if (errno != EEXIST) {
				warn("mkdir(%s)", out);
				return (-1);
			}
		}
		free(out);
		return (0);
	}

	fprintf(stderr, "ERROR: not a file/dir: \"%s\"\n", path);
	return (-1);
}

void
thrlog(char *format, ...)
{
	int delta = (int)((gethrtime() - start) / 1000000);
	char fmt[256];

	(void) snprintf(fmt, sizeof (fmt), "%8d t%02d: %s\n", delta,
	    thr_self(), format);

	va_list ap;
	va_start(ap, format);
	(void) vfprintf(stdout, fmt, ap);
	va_end(ap);
}

static void *
copy_thread(void *arg)
{
	copy_thread_t *ct = arg;

#if 0
	thrlog("alive");
#endif

	while (g_starter_gun == 0) {
		sleep(1);
	}

	for (;;) {
		const char *path;

		if (g_error_stop) {
			thrlog("stopping because of errors");
			goto bail;
		}

		mutex_enter(&g_files_lock);
		path = strset_cursor_peek(g_files_cursor);
		strset_cursor_next(g_files_cursor);
		mutex_exit(&g_files_lock);

		if (path == NULL) {
			break;
		}

		char *path_src, *path_dst;

		if (asprintf(&path_src, "%s/%s", input, path) < 0 ||
		    asprintf(&path_dst, "%s/%s", output, path) < 0) {
			err(1, "asprintf");
		}

#if 0
		thrlog("copy \"%s\" -> \"%s\"", path_src, path_dst);
#endif

		if (builder_copy_file(path_src, path_dst, &ct->ct_progress) !=
		    0) {
			thrlog("copy file error: %s\n\t\tpath: %s",
			    strerror(errno),
			    path_dst);
			g_error_stop = 1;
		}

		free(path_src);
		free(path_dst);

#if 0
		thrlog("end path \"%s\"", path);
#endif
	}

#if 0
	thrlog("no more work; exiting");
#endif

bail:
	atomic_dec_64(&g_running);

	return (NULL);
}

static void
walk_tree(const char *path)
{
	fprintf(stdout, " * walking tree \"%s\"\n", path);

	if (nftw(path, walk_tree_func, 64, FTW_PHYS) != 0) {
		warn("could not walk tree");
		return;
	}

	fprintf(stdout, " * walk ok (%d files to copy)\n",
	    strset_count(g_files));

	if (strset_cursor(g_files, &g_files_cursor) != 0) {
		err(1, "could not get cursor");
		return;
	}

	g_running = g_nthreads;
	membar_producer();

	g_threads = calloc(g_nthreads, sizeof (*g_threads));

	for (unsigned n = 0; n < g_nthreads; n++) {
		copy_thread_t *ct = &g_threads[n];

		if (thr_create(NULL, 0, copy_thread, ct, 0, &ct->ct_id) !=
		    0) {
			err(1, "thr_create");
		}
	}

	g_starter_gun = 1;

	hrtime_t last_sample = 0;
	uint64_t total = 0;
	for (;;) {
		if (g_running == 0) {
			fprintf(stdout, "all threads done\n");
			break;
		}

		uint64_t this_total = 0;

		for (unsigned n = 0; n < g_nthreads; n++) {
			copy_thread_t *ct = &g_threads[n];

			this_total += atomic_swap_64(&ct->ct_progress, 0);
		}

		total += this_total;

		hrtime_t now = gethrtime();
		if (last_sample == 0) {
			goto skip_progress;
		}

		hrtime_t delta = now - last_sample;
		float rate = (float)this_total / ((float)delta /
		    1000000000.0) / 1024.0 / 1024.0;
		float pct = 100.0 * total / (g_files_total_size + 1);
		float sofar_gb = (float)total / 1024.0 / 1024.0 / 1024.0;
		float all_gb = (float)g_files_total_size / 1024.0 / 1024.0 /
		    1024.0;

		thrlog("rate %.1f MB/s tot. %.2f / %.2f GB (%.2f%%) [%d thrs]",
		    rate, sofar_gb, all_gb, pct, (int)g_running);

skip_progress:
		last_sample = now;
		sleep(10);
	}

	/*
	 * Collect final totals.
	 */
	for (unsigned n = 0; n < g_nthreads; n++) {
		copy_thread_t *ct = &g_threads[n];

		total += atomic_swap_64(&ct->ct_progress, 0);
	}
	thrlog("final copy size: %lld MB", (total / 1024 / 1024));

	for (unsigned n = 0; n < g_nthreads; n++) {
		copy_thread_t *ct = &g_threads[n];

		if (thr_join(ct->ct_id, NULL, NULL) != 0) {
			err(1, "thr_join(%d)", ct->ct_id);
		}
	}

	strset_cursor_free(g_files_cursor);
}

static int
check_path(const char *path)
{
	int failed = 0;

	if (path[0] != '/') {
		fprintf(stderr, "require a fully qualified path\n");
		failed = 1;
	}
	if (path[strlen(path) - 1] == '/') {
		fprintf(stderr, "path must not end in \"/\"\n");
		failed = 1;
	}

	return (failed);
}

int
main(int argc, char *argv[])
{
	int c;
	int failed = 0;

	start = gethrtime();

	while ((c = getopt(argc, argv, ":j:")) != -1) {
		switch (c) {
		case 'j':
			g_nthreads = atoi(optarg);
			if (g_nthreads < 1 || g_nthreads > 1000) {
				fprintf(stderr, "ERROR: invalid thread "
				    "count \"%s\"\n", optarg);
				failed = 1;
			}
			break;

		case '?':
			fprintf(stderr, "ERROR: unrecognised option -%c\n",
			    optopt);
			failed = 1;
			break;

		case ':':
			fprintf(stderr, "ERROR: option -%c requires an "
			    "operand\n", optopt);
			failed = 1;
			break;
		}
	}

	if (optind != argc - 2) {
		fprintf(stderr, "require 2 arguments\n");
		failed = 1;
	} else {
		input = argv[optind];
		output = argv[optind + 1];

		failed |= check_path(input);
		failed |= check_path(output);
	}

	if (failed) {
		fprintf(stderr, "usage: %s <input_dir>\n", argv[0]);
		exit(1);
	}

	fprintf(stdout, "input directory:  \"%s\"\n", input);
	fprintf(stdout, "output directory: \"%s\"\n", output);

	if (mkdir(output, 0755) != 0) {
		if (errno != EEXIST) {
			err(1, "mkdir(%s)", output);
		}
	}

	if (strset_alloc(&g_files, 0) != 0) {
		err(1, "strset_alloc");
	}

	walk_tree(input);

	return (g_error_stop);
}
