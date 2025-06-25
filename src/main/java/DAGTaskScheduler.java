import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务调度框架 - 支持Cron表达式和DAG依赖
 */
public class DAGTaskScheduler {
    public interface Task {
        String getId();

        void execute();
    }

    // 带依赖关系的任务包装类
    public static class TaskNode {
        private final Task task;
        private final List<TaskNode> dependencies = Lists.newArrayList();
        private final List<TaskNode> dependents = Lists.newArrayList();
        private final AtomicInteger pendingDependencies = new AtomicInteger(0);

        public TaskNode(Task task) {
            this.task = task;
        }

        public void addDependency(TaskNode node) {
            dependencies.add(node);
            node.dependents.add(this);
        }

        public List<TaskNode> getDependencies() {
            return Collections.unmodifiableList(dependencies);
        }

        public void reset() {
            pendingDependencies.set(dependencies.size());
        }
    }

    // 调度器核心
    public static class Scheduler {
        private final ScheduledExecutorService cronExecutor = Executors.newScheduledThreadPool(4);
        private final ExecutorService taskExecutor = Executors.newWorkStealingPool();
        private final Map<String, TaskNode> taskGraph = Maps.newConcurrentMap();
        private final Map<String, ScheduledFuture<?>> cronJobs = Maps.newConcurrentMap();
        private final int COMMON_TASK_TYPE = 1;
        private final int SCHEDULE_TASK_TYPE = 2;

        // 添加任务到DAG
        public void addTask(Task task, List<String> dependencyIds) {
            TaskNode node = new TaskNode(task);
            taskGraph.put(task.getId(), node);

            for (String depId : dependencyIds) {
                TaskNode depNode = taskGraph.get(depId);
                if (depNode != null) {
                    node.addDependency(depNode);
                }
            }
        }

        // 添加Cron任务
        public void scheduleCronTask(Task task, String cronExpression) {
            CronExpression cron = new CronExpression(cronExpression);
            Runnable job = () -> {
                if (canExecute(task.getId(), SCHEDULE_TASK_TYPE)) {
                    submitTask(task);
                }
            };

            ScheduledFuture<?> future = cronExecutor.scheduleAtFixedRate(
                    job,
                    cron.nextDelay(TimeUnit.SECONDS),
                    cron.nextDelay(TimeUnit.SECONDS),
                    TimeUnit.SECONDS
            );

            cronJobs.put(task.getId(), future);
        }

        // 提交任务执行
        private void submitTask(Task task) {
            taskExecutor.submit(() -> {
                try {
                    task.execute();
                    onTaskComplete(task.getId());
                } catch (Exception e) {
                    handleTaskFailure(task.getId(), e);
                }
            });
        }

        // 任务完成处理
        private void onTaskComplete(String taskId) {
            TaskNode node = taskGraph.get(taskId);
            if (node != null) {
                for (TaskNode dependent : node.dependents) {
                    if (dependent.pendingDependencies.decrementAndGet() == 0) {
                        submitTask(dependent.task);
                    }
                }
            }
        }

        // 检查任务是否可执行
        private boolean canExecute(String taskId, int taskType) {
            if (taskType == COMMON_TASK_TYPE) {
                TaskNode node = taskGraph.get(taskId);
                return node != null && node.pendingDependencies.get() == 0;
            } else if (taskType == SCHEDULE_TASK_TYPE) {
                return cronJobs.keySet().stream().anyMatch(taskId::equals);
            }
            return false;
        }

        // 失败处理
        private void handleTaskFailure(String taskId, Exception e) {
            // 实现重试或报警逻辑
            System.err.println("Task failed: " + taskId + ", Error: " + e.getMessage());
        }

        // 启动DAG调度
        public void start() {
            // 初始化依赖计数器
            for (TaskNode node : taskGraph.values()) {
                node.reset();
            }

            // 提交所有无依赖的任务
            for (TaskNode node : taskGraph.values()) {
                if (node.pendingDependencies.get() == 0) {
                    submitTask(node.task);
                }
            }
        }

        // 停止调度器
        public void shutdown() {
            cronExecutor.shutdown();
            taskExecutor.shutdown();
            cronJobs.values().forEach(future -> future.cancel(true));
        }
    }

    // Cron表达式解析器（简化版）
    public static class CronExpression {
        private static final Map<String, String> MONTH_MAP = Map.ofEntries(
                Map.entry("JAN", "1"), Map.entry("FEB", "2"),
                Map.entry("MAR", "3"), Map.entry("APR", "4"),
                Map.entry("MAY", "5"), Map.entry("JUN", "6"),
                Map.entry("JUL", "7"), Map.entry("AUG", "8"),
                Map.entry("SEP", "9"), Map.entry("OCT", "10"),
                Map.entry("NOV", "11"), Map.entry("DEC", "12")
        );

        private static final Map<String, String> WEEKDAY_MAP = Map.of(
                "SUN", "0", "MON", "1", "TUE", "2", "WED", "3",
                "THU", "4", "FRI", "5", "SAT", "6"
        );

        private final String expression;
        private final String[] parts;

        public CronExpression(String expression) {
            this.expression = normalize(expression);
            this.parts = this.expression.split("\\s+");
            if (parts.length != 6) {
                throw new IllegalArgumentException("Invalid cron expression format");
            }
        }

        private String normalize(String expr) {
            String normalized = expr.toUpperCase()
                    .replaceAll("\\?", "*")
                    .replaceAll("\\s+", " ")
                    .trim();

            for (Map.Entry<String, String> entry : MONTH_MAP.entrySet()) {
                normalized = normalized.replace(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, String> entry : WEEKDAY_MAP.entrySet()) {
                normalized = normalized.replace(entry.getKey(), entry.getValue());
            }

            return normalized;
        }

        // 计算下次执行延迟
        public long nextDelay(TimeUnit unit) {
            // 简化实现：返回固定间隔
            if (parts[0].equals("*") && parts[1].equals("*")) {
                return unit.convert(1, TimeUnit.MINUTES); // 每分钟
            }
            if (parts[0].startsWith("*/")) {    //间隔多少秒执行
                int interval = Integer.parseInt(parts[0].substring(2));
                return unit.convert(interval, TimeUnit.SECONDS);
            }
            return unit.convert(1, TimeUnit.HOURS); // 默认1小时
        }
    }

    // 示例使用
    public static void main(String[] args) throws InterruptedException {
        Scheduler scheduler = new Scheduler();

        // 创建任务
        Task dataLoadTask = createTask("DATA_LOAD", "Load data");
        Task processTask = createTask("PROCESS", "Process data");
        Task reportTask = createTask("REPORT", "Generate report");
        Task cleanupTask = createTask("CLEANUP", "Clean resources");

        // 构建DAG
        scheduler.addTask(dataLoadTask, Collections.emptyList());
        scheduler.addTask(processTask, List.of("DATA_LOAD"));
        scheduler.addTask(reportTask, List.of("PROCESS"));
        scheduler.addTask(cleanupTask, List.of("REPORT", "PROCESS"));

        // 添加定时任务
        scheduler.scheduleCronTask(createTask("DAILY_BACKUP", "Backup data"), "*/2 * * * * *");

        // 启动调度
        scheduler.start();

        // 运行10s后关闭
        Thread.sleep(10_000);
        scheduler.shutdown();
    }

    private static Task createTask(String id, String name) {
        return new Task() {
            @Override
            public String getId() {
                return id;
            }

            @Override
            public void execute() {
                System.out.printf("[%tT] Executing %s: %s%n",
                        System.currentTimeMillis(), id, name);
                // 模拟任务执行时间
                try {
                    Thread.sleep(new Random().nextInt(1500));
                } catch (InterruptedException ignored) {
                }
            }
        };
    }
}