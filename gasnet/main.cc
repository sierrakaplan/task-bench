/* Copyright 2019 Stanford University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include <gasnet.h>
#include <gasnet_coll.h>

#include "core.h"

#define GASNET_BLOCKUNTIL(cond) while (!(cond)) gasnet_AMPoll()

static const int MAX_HANDLERS = 10;
static const int REQUEST_HANDLER_ID = 0;
static const int RESPONSE_HANDLER_ID = 1;
static gasnet_handlerentry_t handlers[MAX_HANDLERS];
static int hcount = 0;

void add_handler_entry(int msgid, void (*fnptr)())
{
  assert(hcount < MAX_HANDLERS);
  handlers[hcount].index = msgid;
  handlers[hcount].fnptr = fnptr;
  hcount++;
}

int main(int argc, char *argv[])
{
	gasnet_init(argc, argv);
	int n_ranks = gasnet_nodes();
	int rank = gasnet_mynode();
	uintptr_t segsize = gasnet_getMaxLocalSegmentSize();

	App app(argc, argv);
	if (rank == 0) app.display();

  add_handler_entry(REQUEST_HANDLER_ID, request_handler_medium);
	gasnet_attach(handlers, hcount, segsize, 0);

	for (auto graph : app.graphs) {
		long first_point = rank * graph.max_width / n_ranks;
   	long last_point = (rank + 1) * graph.max_width / n_ranks - 1;
   	long n_points = last_point - first_point + 1;

   	size_t scratch_bytes = graph.scratch_bytes_per_task;
    char *scratch_ptr = (char *)malloc(scratch_bytes * n_points);
    assert(scratch_ptr);

  	std::vector<int> rank_by_point(graph.max_width);
  	std::vector<int> tag_bits_by_point(graph.max_width);
  	for (int r = 0; r < n_ranks; ++r) {
    	long r_first_point = r * graph.max_width / n_ranks;
    	long r_last_point = (r + 1) * graph.max_width / n_ranks - 1;
    	for (long p = r_first_point; p <= r_last_point; ++p) {
      		rank_by_point[p] = r;
      		tag_bits_by_point[p] = p - r_first_point;
    	}
  	}

  	long max_deps = 0;
    for (long dset = 0; dset < graph.max_dependence_sets(); ++dset) {
      for (long point = first_point; point <= last_point; ++point) {
        long deps = 0;
        for (auto interval : graph.dependencies(dset, point)) {
          deps += interval.second - interval.first + 1;
        }
        max_deps = std::max(max_deps, deps);
      }
    }

    // Create input and output buffers.
    std::vector<std::vector<std::vector<char> > > inputs(n_points);
    std::vector<std::vector<const char *> > input_ptr(n_points);
    std::vector<std::vector<size_t> > input_bytes(n_points);
    std::vector<long> n_inputs(n_points);
    std::vector<std::vector<char> > outputs(n_points);
    for (long point = first_point; point <= last_point; ++point) {
      long point_index = point - first_point;

      auto &point_inputs = inputs[point_index];
      auto &point_input_ptr = input_ptr[point_index];
      auto &point_input_bytes = input_bytes[point_index];

      point_inputs.resize(max_deps);
      point_input_ptr.resize(max_deps);
      point_input_bytes.resize(max_deps);

      for (long dep = 0; dep < max_deps; ++dep) {
        point_inputs[dep].resize(graph.output_bytes_per_task);
        point_input_ptr[dep] = point_inputs[dep].data();
        point_input_bytes[dep] = point_inputs[dep].size();
      }

      auto &point_outputs = outputs[point_index];
      point_outputs.resize(graph.output_bytes_per_task);
    }

    // Cache dependencies.
    std::vector<std::vector<std::vector<std::pair<long, long> > > > dependencies(graph.max_dependence_sets());
    std::vector<std::vector<std::vector<std::pair<long, long> > > > reverse_dependencies(graph.max_dependence_sets());
    for (long dset = 0; dset < graph.max_dependence_sets(); ++dset) {
      dependencies[dset].resize(n_points);
      reverse_dependencies[dset].resize(n_points);

      for (long point = first_point; point <= last_point; ++point) {
        long point_index = point - first_point;

        dependencies[dset][point_index] = graph.dependencies(dset, point);
        reverse_dependencies[dset][point_index] = graph.reverse_dependencies(dset, point);
      }
    }

    for (long timestep = 0; timestep < graph.timesteps; ++timestep) {
      long offset = graph.offset_at_timestep(timestep);
      long width = graph.width_at_timestep(timestep);

      long last_offset = graph.offset_at_timestep(timestep-1);
      long last_width = graph.width_at_timestep(timestep-1);

      long dset = graph.dependence_set_at_timestep(timestep);
      auto &deps = dependencies[dset];
      auto &rev_deps = reverse_dependencies[dset];

      requests.clear();

      for (long point = first_point; point <= last_point; ++point) {
        long point_index = point - first_point;

        auto &point_inputs = inputs[point_index];
        auto &point_n_inputs = n_inputs[point_index];
        auto &point_output = outputs[point_index];

        auto &point_deps = deps[point_index];
        auto &point_rev_deps = rev_deps[point_index];

        /* Request data from dependencies (MPI Receive equivalent) */
        point_n_inputs = 0;
        if (point >= offset && point < offset + width) {
          for (auto interval : point_deps) {
            for (long dep = interval.first; dep <= interval.second; ++dep) {
              if (dep < last_offset || dep >= last_offset + last_width) {
                continue;
              }

              // Use shared memory for on-node data.
              if (first_point <= dep && dep <= last_point) {
                auto &output = outputs[dep - first_point];
                point_inputs[point_n_inputs].assign(output.begin(), output.end());
              } else {
                int from = tag_bits_by_point[dep];
                int to = tag_bits_by_point[point];
                int tag = (from << 8) | to;

                // dest node, handler, data, nbytes, args
                gasnet_AMRequestMediumM(rank_by_point[dep], 
                        handlers[REQUEST_HANDLER_ID],
                        point_inputs[point_n_inputs].data(),
                        point_inputs[point_n_inputs].size());

                requests.push_back(req);
              }
              point_n_inputs++;
            }
          }
        }

        /* Send data to tasks that depend on this (MPI Send equivalent) */
        if (point >= last_offset && point < last_offset + last_width) {
          for (auto interval : point_rev_deps) {
            for (long dep = interval.first; dep <= interval.second; dep++) {
              if (dep < offset || dep >= offset + width || (first_point <= dep && dep <= last_point)) {
                continue;
              }

              int from = tag_bits_by_point[point];
              int to = tag_bits_by_point[dep];
              
            }
          }
        }
      }

      GASNET_BLOCKUNTIL(num_responses == num_requests);

      for (long point = std::max(first_point, offset); point <= std::min(last_point, offset + width - 1); ++point) {
        long point_index = point - first_point;

        auto &point_input_ptr = input_ptr[point_index];
        auto &point_input_bytes = input_bytes[point_index];
        auto &point_n_inputs = n_inputs[point_index];
        auto &point_output = outputs[point_index];

        graph.execute_point(timestep, point,
                            point_output.data(), point_output.size(),
                            point_input_ptr.data(), point_input_bytes.data(), point_n_inputs,
                            scratch_ptr + scratch_bytes * point_index, scratch_bytes);
      }
    }
    free(scratch_ptr);

	}

	gasnet_exit();
}

static void request_handler_medium(gasnet_token_t token, void *buf, size_t nbytes) {
  gasnet_AMReplyMediumM(token, handler, data, size)

}

static void reply_handler_medium(gasnet_token_t token, void *buf, size_t nbytes) {

}