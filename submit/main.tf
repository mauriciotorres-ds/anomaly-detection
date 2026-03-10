# DS5220 Data Project 1 - Terraform Solution
# Provisions the same resources as the CloudFormation template:
# EC2, S3, IAM role, Security Group, Elastic IP, SNS topic, S3 event notification

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Provider
provider "aws" {
  region = "us-east-1"
}


# Variables
variable "my_ip" {
  description = "Your local IP address for SSH access (CIDR notation)"
  default     = "199.111.224.14/32"
}

variable "bucket_name" {
  description = "Name of the S3 bucket for raw uploads, processed output, state, and logs"
  default     = "ds5220-dp1-terraform-mauricio"
}

variable "github_repo" {
  description = "URL of your forked anomaly-detection repository"
  default     = "https://github.com/mauriciotorres-ds/anomaly-detection"
}

variable "key_name" {
  description = "Name of an existing EC2 key pair for SSH access"
  default     = "ds5220-dp1-key"
}


# IAM Role and Instance Profile
resource "aws_iam_role" "ec2_role" {
  name = "ds5220-dp1-tf-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "s3_policy" {
  name = "ds5220-dp1-tf-s3-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "ds5220-dp1-tf-instance-profile"
  role = aws_iam_role.ec2_role.name
}


# Security Group
resource "aws_security_group" "anomaly_sg" {
  name        = "ds5220-dp1-tf-sg"
  description = "DS5220 DP1 Terraform - Allow SSH from my IP and port 8000 from anywhere"

  # SSH from my IP only
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
    description = "SSH from my IP only"
  }

  # FastAPI from anywhere
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "FastAPI from anywhere"
  }

  # allow all outbound traffic so the instance can reach S3, GitHub, etc.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# SNS Topic
resource "aws_sns_topic" "anomaly_topic" {
  name = "ds5220-dp1"
}

# SNS Topic Policy - allows S3 to publish to this topic
resource "aws_sns_topic_policy" "anomaly_topic_policy" {
  arn = aws_sns_topic.anomaly_topic.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3ToPublish"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.anomaly_topic.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = "arn:aws:s3:::${var.bucket_name}"
          }
        }
      }
    ]
  })
}


# S3 Bucket
resource "aws_s3_bucket" "anomaly_bucket" {
  bucket = var.bucket_name

  # making sure the SNS topic policy exists before creating the bucket
  # so S3 can publish notifications immediately
  depends_on = [aws_sns_topic_policy.anomaly_topic_policy]
}

# S3 event notification — only trigger on raw/*.csv uploads to avoid recursion
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.anomaly_bucket.id

  topic {
    topic_arn     = aws_sns_topic.anomaly_topic.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
    filter_suffix = ".csv"
  }

  depends_on = [aws_sns_topic_policy.anomaly_topic_policy]
}

# EC2 Instance
resource "aws_instance" "anomaly_ec2" {
  # Ubuntu 24.04 LTS (us-east-1)
  ami                  = "ami-0c7217cdde317cfec"
  instance_type        = "t3.micro"
  key_name             = var.key_name
  iam_instance_profile = aws_iam_instance_profile.ec2_profile.name
  security_groups      = [aws_security_group.anomaly_sg.name]

  root_block_device {
    volume_size = 16
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e

    # system update and dependencies
    apt-get update -y
    apt-get install -y git python3 python3-pip python3-venv

    # set BUCKET_NAME globally and for this session
    echo 'BUCKET_NAME="${var.bucket_name}"' >> /etc/environment
    export BUCKET_NAME="${var.bucket_name}"

    # clone the forked repo
    git clone ${var.github_repo} /opt/anomaly-detection

    # create virtual environment and install dependencies
    python3 -m venv /opt/anomaly-detection/venv
    /opt/anomaly-detection/venv/bin/pip install --upgrade pip
    /opt/anomaly-detection/venv/bin/pip install -r /opt/anomaly-detection/requirements.txt

    # create a systemd service so FastAPI starts on boot
    cat <<SYSTEMD > /etc/systemd/system/anomaly-detection.service
    [Unit]
    Description=Anomaly Detection FastAPI Service
    After=network.target

    [Service]
    User=root
    WorkingDirectory=/opt/anomaly-detection
    EnvironmentFile=/etc/environment
    ExecStart=/opt/anomaly-detection/venv/bin/fastapi run /opt/anomaly-detection/app.py
    Restart=always
    RestartSec=5

    [Install]
    WantedBy=multi-user.target
    SYSTEMD

    systemctl daemon-reload
    systemctl enable anomaly-detection
    systemctl start anomaly-detection
  EOF

  tags = {
    Name = "ds5220-dp1-tf-instance"
  }
}


# Elastic IP and Association
resource "aws_eip" "anomaly_eip" {
  domain = "vpc"
}

resource "aws_eip_association" "eip_assoc" {
  instance_id   = aws_instance.anomaly_ec2.id
  allocation_id = aws_eip.anomaly_eip.id
}

# SNS Subscription - HTTP to EC2 /notify endpoint
# depends on EIP association so the IP is ready before subscribing
resource "aws_sns_topic_subscription" "anomaly_subscription" {
  topic_arn              = aws_sns_topic.anomaly_topic.arn
  protocol               = "http"
  endpoint               = "http://${aws_eip.anomaly_eip.public_ip}:8000/notify"
  endpoint_auto_confirms = true

  depends_on = [aws_eip_association.eip_assoc]
}

# Outputs
output "instance_public_ip" {
  description = "Elastic IP of the EC2 instance"
  value       = aws_eip.anomaly_eip.public_ip
}

output "api_endpoint" {
  description = "FastAPI base URL"
  value       = "http://${aws_eip.anomaly_eip.public_ip}:8000"
}

output "notify_endpoint" {
  description = "SNS subscription endpoint"
  value       = "http://${aws_eip.anomaly_eip.public_ip}:8000/notify"
}

output "s3_bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.anomaly_bucket.bucket
}

output "sns_topic_arn" {
  description = "SNS topic ARN"
  value       = aws_sns_topic.anomaly_topic.arn
}
