# Project instructions

## 发票文件夹：只可新建，不可修改或删除（最高优先级）

适用文件夹：`C:\google drive\Ausway\Tax invoice\Received`
（即 Google Drive 中的 `Ausway / Tax invoice / Received`，包括其子文件夹）

**允许：**
- 查看文件列表，读取、分析已有文件（例如从旧发票里读取代理的 ABN、地址和银行信息，或查最大发票号）
- **生成并新建**文件，例如新的发票 `.xlsx` / `.pdf`

**禁止**对文件夹中任何已有的文件或文件夹（包括 `AGENTS.md`）执行：
- 修改或编辑内容、覆盖、另存为同名文件
- 删除或移到回收站
- 重命名、移动
- 批量整理、清理、格式化或转换

**执行要求：**
- 新建文件前，先确认同名文件不存在。如果已经存在，不要覆盖，停下来问用户。
- 新文件先在本地生成，给用户确认内容后，再在该文件夹中新建。
- 如果用户的请求需要修改或删除已有文件，拒绝执行，说明违反了本规则，并建议用户自己手动操作。

## 发票标准信息（2026-11-07 起）

- 抬头公司名称统一为 **Ausway Education Group Pty Ltd**
- 公司 ABN **51 676 830 363**，ACN 676 830 363
- 公司地址统一用现址：**Level 1, 63-73 Ann Street, Surry Hills, NSW 2010**
- 以上存于 `invoice/company.json`，生成发票时自动使用，不从旧发票复制
- 旧发票中 BSB 与账号栏位填反的，新发票一律改正（生成器自动纠正并提示）
- **GST：** 代理没有注册 GST（旧发票写 GST: N/A）时，标题用 **Invoice**；注册了的，用 **Tax Invoice** 并列出 GST。
  **每次遇到新代理，必须先问用户该代理是否注册 GST，确认后才能开票。**
- **佣金比例：** 按用户给的 professional service 信息决定写法。给出了各段比例的，写成过程比例（如 `10%*60%`）；
  只有最终比例的，写最终比例（如 `8.24%`）。不要自行换算或合并。
- 发票底部保留 "Name of the Agent" 和 "Signatory" 签字行。
- **金额以用户表格为准：** 应付金额与「计佣学费 × 比例」相差 **1 元以内**属于 rounding，直接用表上金额，不必再问用户。
  相差 1 元或以上才需要确认。（表上比例明显错位时，例如 87.20% 实为 8.72%，仍需确认。）
- **重复检查：** 开票前对照该代理以往的发票。同一学生已开过的，先问用户是否为后续一期佣金。

## Invoice folder: create-only (English summary)
For the folder above you may list and read files, and create **new** files only. Never modify,
overwrite, rename, move, trash or delete any existing file or folder there. If a target name already
exists, stop and ask. The generator (`invoice/make_invoice.py`) refuses to overwrite existing output.
