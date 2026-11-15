// 子进程重启演练：只凭 stateDir 恢复，不接收任何调用方内存状态
import { run } from '../src/workflow.mjs';

const [source, stateDir] = process.argv.slice(2);
if (!source || !stateDir) {
  console.error('用法：node restart-runner.mjs <source> <stateDir>');
  process.exit(2);
}
try {
  // 重启后传入相同的业务输入：已完成项应直接复用落盘状态，不再访问来源
  const result = await run({ source, stateDir, input: { transport: 'h2', items: ['plain'], claims: ['paid'] } });
  console.log(JSON.stringify(result));
} catch (error) {
  console.error(error?.stack ?? error);
  process.exit(1);
}
