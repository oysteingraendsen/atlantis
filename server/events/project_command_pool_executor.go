package events

import (
	"fmt"
	"sort"
	"sync"

	"github.com/remeh/sizedwaitgroup"
	"github.com/runatlantis/atlantis/server/events/command"
)

type prjCmdRunnerFunc func(ctx command.ProjectContext) command.ProjectResult

func runProjectCmdsParallel(
	ctx *command.Context,
	cmds []command.ProjectContext,
	runnerFunc prjCmdRunnerFunc,
	poolSize int,
) command.Result {
	var results []command.ProjectResult
	mux := &sync.Mutex{}
	ctx.Log.Info("DEBUG: in runProjectCmdsParallelGroups")

	wg := sizedwaitgroup.New(poolSize)
	for _, pCmd := range cmds {
		pCmd := pCmd
		var execute func()
		wg.Add()

		execute = func() {
			defer wg.Done()
			res := runnerFunc(pCmd)
			mux.Lock()
			results = append(results, res)
			mux.Unlock()
		}

		go execute()
	}

	wg.Wait()
	return command.Result{ProjectResults: results}
}

func runProjectCmds(
	cmds []command.ProjectContext,
	runnerFunc prjCmdRunnerFunc,
) command.Result {
	var results []command.ProjectResult
	for _, pCmd := range cmds {
		res := runnerFunc(pCmd)

		results = append(results, res)
	}
	return command.Result{ProjectResults: results}
}

func splitByExecutionOrderGroup(cmds []command.ProjectContext) [][]command.ProjectContext {
	groups := make(map[int][]command.ProjectContext)
	for _, cmd := range cmds {
		groups[cmd.ExecutionOrderGroup] = append(groups[cmd.ExecutionOrderGroup], cmd)
	}

	var groupKeys []int
	for k := range groups {
		groupKeys = append(groupKeys, k)
	}
	sort.Ints(groupKeys)

	var res [][]command.ProjectContext
	for _, group := range groupKeys {
		res = append(res, groups[group])
	}
	return res
}

func runProjectCmdsParallelGroups(
	ctx *command.Context,
	cmds []command.ProjectContext,
	runnerFunc prjCmdRunnerFunc,
	poolSize int,
) command.Result {
	ctx.Log.Info("DEBUG: in runProjectCmdsParallelGroups")
	var results []command.ProjectResult
	groups := splitByExecutionOrderGroup(cmds)
	for _, group := range groups {
		res := runProjectCmdsParallel(ctx, group, runnerFunc, poolSize)
		results = append(results, res.ProjectResults...)
		for _, r := range res.ProjectResults {
			ctx.Log.Info(
				fmt.Sprintf("projectId: %s \n plan status: %s \n failure: %s \n error: %s \n command: %d (Plan is 1)",
				r.Workspace, r.PlanStatus().String(), r.Failure, r.Error, r.Command),
			)
		}
		if res.HasErrors() && group[0].AbortOnExcecutionOrderFail {
			ctx.Log.Info("abort on execution order when failed")
			break
		} else {
			ctx.Log.Info(fmt.Sprintf("no errors in the %d results", len(res.ProjectResults)))
		}
	}

	return command.Result{ProjectResults: results}
}
