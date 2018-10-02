/* Copyright 2018 Stanford University
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

use Time;
use BlockDist;
config const quiet: bool = false;
var t: Timer;

extern {

  #include "core_c.h"

}

proc main(args: [] string) {
  writeln("in main");
  stdout.flush();
  var argc = args.numElements;
  var app = app_create(argc:int(32), convert_args_to_c_args(argc, args));
  var graph_list = app_task_graphs(app); // array of tasks grapsh 
  app_display(app);

  var n_graphs = task_graph_list_num_task_graphs(graph_list);
  var graphs: [0..n_graphs-1] task_graph_t;
  for i in 0..n_graphs-1 {
    graphs[i] = task_graph_list_task_graph(graph_list, i);
  }

  // For simplicity, we're going to allocate one giant domain for
  // task results and completion variables, so the domain has to be
  // the upper bound of all the tasks we'll need.
  var max_width: int(64) = 0;
  var max_timesteps: int(64) = 0;
  var max_output_bytes: int(64) = 0;
  for graph in graphs {
    max_width = max(max_width, graph.max_width: int(64));
    max_timesteps = max(max_timesteps, graph.timesteps: int(64));
    max_output_bytes = max(max_output_bytes, graph.output_bytes_per_task: int(64));
  }

  var task_result = make_task_result(n_graphs, max_width, max_output_bytes);
  var task_completed = make_task_completed(n_graphs, max_width, max_timesteps);
  var task_used = make_task_completed(n_graphs, max_width, max_timesteps);

  execute_task_graphs(graphs, task_result, task_completed, task_used);
}

proc make_task_result(n_graphs, max_width, max_output_bytes) {
  const space = {0..n_graphs-1, 0..max_width-1, 0..max_output_bytes-1};
  const locale_space = {0..0, 0..numLocales-1, 0..0};
  var targets: [locale_space] locale;
  forall i in 0..numLocales-1 {
    targets[0, i, 0] = Locales[i];
  }
  const D: domain(3) dmapped Block(boundingBox=space, targetLocales=targets) = space;
  var result: [D] int(8);
  return result;
}

proc make_task_completed(n_graphs, max_width, max_timesteps) {
  const space = {0..n_graphs-1, 0..max_width-1, 0..max_timesteps-1};
  const locale_space = {0..0, 0..numLocales-1, 0..0};
  var targets: [locale_space] locale;
  forall i in 0..numLocales-1 {
    targets[0, i, 0] = Locales[i];
  }
  const D: domain(3) dmapped Block(boundingBox=space, targetLocales=targets) = space;
  var result: [D] atomic int(64);
  return result;
}

proc execute_task_graphs(graphs, task_result, task_completed, task_used) {
  coforall graph in graphs {
    coforall loc in Locales {
      on loc {
        execute_task_graph2(graph, task_result, task_completed, task_used);
      }
    }
  }
}

proc execute_task_graph2(graph, task_result, task_completed, task_used) {
  const graph_index = graph.graph_index;

  writeln("running graph ", graph_index, " on locale ", here.id);
  writeln("  in chapel: graph_index ", graph_index, " timesteps ", graph.timesteps, " max_width ", graph.max_width, " dtype ", graph.dependence, " (on locale ", here.id, ")");
  stdout.flush();

  writeln("  chapel is about to call C");
  stdout.flush();
  var outer_n_dsets = task_graph_max_dependence_sets(graph):int(64);
  writeln("  chapel returned from call to C");
  stdout.flush();
}

proc convert_args_to_c_args(argc, args) {
  var result = c_malloc(c_ptr(int(8)), argc + 1);
 	  // not efficent but needed to convert args
  for i in 0..argc - 1 {
 		  // make c memeory for each word
    var curr = c_malloc(int(8), args[i].length + 1);
 		  // loop over each character to add it to a string 
    var j = 0;
    for chr in args[i] {
      curr[j] = ascii(chr):int(8);
      j += 1;
    }
    assert(j == args[i].length);
    curr[j] = 0;
    result[i] = curr;
  }
  result[argc] = nil;
  return result;
}
