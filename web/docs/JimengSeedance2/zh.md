### Jimeng Seedance 2.0
高级视频生成节点，支持文本、图片、视频、音频多模态参考，可覆盖文生视频、多模态参考生视频、视频编辑与视频延长等工作流。

*   **Node ID**: `JimengSeedance2`
*   **Python Class**: `JimengSeedance2`

#### 输入 (Inputs)

| 参数名 | 类型 | 必填 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- | :--- |
| `client` | `JIMENG_CLIENT` | 是 | - | API 客户端。 |
| `model_version` | `COMBO` | 是 | `doubao-seedance-2-0` | 视频模型版本。 |
| `prompt` | `STRING` | 否 | `""` | 视频提示词。 |
| `enable_random_seed` | `BOOLEAN` | 是 | `True` | 是否启用随机种子 (覆盖 `seed` 参数)。 |
| `seed` | `INT` | 是 | `0` | 种子值。 |
| `resolution` | `COMBO` | 是 | `"720p"` | 分辨率 (480p, 720p)。 |
| `aspect_ratio` | `COMBO` | 是 | `"adaptive"` | 宽高比 (adaptive, 21:9, 16:9, 4:3, 1:1, 3:4, 9:16)。 |
| `auto_duration` | `BOOLEAN` | 是 | `False` | 是否自动决定时长 (忽略 `duration` 参数)。 |
| `duration` | `INT` | 是 | `5` | 视频时长 (秒)，范围 4-15。 |
| `generate_audio` | `BOOLEAN` | 是 | `True` | 是否生成音频。 |
| `enable_web_search` | `BOOLEAN` | 是 | `False` | 是否启用联网搜索增强。 |
| `generation_count` | `INT` | 是 | `1` | 批量生成数量。 |
| `filename_prefix` | `STRING` | 是 | `"Jimeng/Video/Batch/Seedance"` | 保存文件的前缀。 |
| `save_last_frame_batch`| `BOOLEAN` | 是 | `False` | 是否单独保存最后一帧。 |
| `non_blocking` | `BOOLEAN` | 是 | `False` | 是否使用非阻塞异步模式 (立即返回，后台轮询)。 |
| `first_frame_image` | `IMAGE` | 否 | - | (可选) 首帧图像。 |
| `last_frame_image` | `IMAGE` | 否 | - | (可选) 尾帧图像 (需同时提供首帧)。 |
| `ref_image` | `IMAGE` | 否 | - | (可选) 多模态参考的图像。 |
| `ref_video` | `VIDEO` | 否 | - | (可选) 多模态参考的视频。 |
| `ref_audio` | `AUDIO` | 否 | - | (可选) 多模态参考的音频。 |

#### 输出 (Outputs)

| 输出名 | 类型 | 描述 |
| :--- | :--- | :--- |
| `video` | `VIDEO` | 生成的视频文件路径对象。 |
| `last_frame` | `IMAGE` | 视频的最后一帧图像。 |
| `response` | `STRING` | 任务响应 JSON。 |
