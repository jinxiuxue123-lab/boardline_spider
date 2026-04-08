#!/bin/zsh

# 文案模型
export AI_PROVIDER="openai_compatible"
export OPENAI_BASE_URL="https://api.n1n.ai"
export OPENAI_API_KEY=""
export OPENAI_MODEL="gpt-5.4-mini"

# 图片模型
export IMAGE_PROVIDER="nanobanana"
export IMAGE_BASE_URL="https://api.n1n.ai"
export IMAGE_API_KEY=""
export IMAGE_MODEL="gemini-3.1-flash-image-preview"

# 阿里云 OSS
export ALIYUN_OSS_ACCESS_KEY_ID=""
export ALIYUN_OSS_ACCESS_KEY_SECRET=""
export ALIYUN_OSS_BUCKET="jinnxiuxue"
export ALIYUN_OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
export XIANYU_IMAGE_CDN_BASE_URL="https://jinnxiuxue.oss-cn-hangzhou.aliyuncs.com"

# 如果你临时还要走本地公网映射，可以保留；不用时留空
export PUBLIC_MEDIA_BASE_URL=""

# 如果后面还要回调，再填这里
export XIANYU_CALLBACK_PUBLIC_URL=""

# 本地淘宝生图后，是否自动把 AI 图元数据推到服务器
export AI_SYNC_AUTO_PUSH="0"

# 自动推送目标服务器
export AI_SYNC_REMOTE_USER="root"
export AI_SYNC_REMOTE_HOST="47.80.63.228"
export AI_SYNC_REMOTE_DIR="/root/boardline_spider"
