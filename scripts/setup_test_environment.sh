#!/bin/bash

echo "🔧 Setting up test environment for pyspark-analyzer..."

# Function to detect Java
detect_java() {
    # Check for JAVA_HOME first
    if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        echo "$JAVA_HOME"
        return 0
    fi

    # Try java_home on macOS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v /usr/libexec/java_home >/dev/null 2>&1; then
            local java_home=$(/usr/libexec/java_home 2>/dev/null)
            if [ -n "$java_home" ] && [ -x "$java_home/bin/java" ]; then
                echo "$java_home"
                return 0
            fi
        fi
    fi

    # Check common Java installation paths - prioritize Java 17 for PySpark 4.0
    local java_paths=(
        "/opt/homebrew/opt/openjdk@17"
        "/usr/local/opt/openjdk@17"
        "/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/adoptopenjdk-17.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home"
        "/usr/lib/jvm/java-17-openjdk"
        "/usr/lib/jvm/java-17-openjdk-amd64"
        "/opt/homebrew/opt/openjdk"
        "/usr/local/opt/openjdk"
        "/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home"
        "/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
        "/usr/lib/jvm/java-11-openjdk"
        "/usr/lib/jvm/java-11-openjdk-amd64"
        "/opt/homebrew/opt/openjdk@11"
        "/usr/local/opt/openjdk@11"
    )

    for path in "${java_paths[@]}"; do
        if [ -d "$path" ] && [ -x "$path/bin/java" ]; then
            echo "$path"
            return 0
        fi
    done

    # Check if java is in PATH
    if command -v java >/dev/null 2>&1; then
        local java_bin=$(command -v java)
        if [ -L "$java_bin" ]; then
            java_bin=$(readlink -f "$java_bin" 2>/dev/null || readlink "$java_bin")
        fi
        local java_home=$(dirname $(dirname "$java_bin"))
        if [ -d "$java_home" ]; then
            echo "$java_home"
            return 0
        fi
    fi

    return 1
}

# Detect Java
echo "📍 Checking Java installation..."
JAVA_HOME_DETECTED=$(detect_java)

if [ -z "$JAVA_HOME_DETECTED" ]; then
    echo "❌ Java 17 or later is not installed on your system!"
    echo ""
    echo "PySpark 4.0 requires Java 17 or later. Here are the recommended ways to install:"
    echo ""
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "On macOS:"
        echo "  1. Using Homebrew (recommended):"
        echo "     brew install openjdk@17"
        echo "     sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk"
        echo ""
        echo "  2. Using SDKMAN:"
        echo "     curl -s \"https://get.sdkman.io\" | bash"
        echo "     source \"$HOME/.sdkman/bin/sdkman-init.sh\""
        echo "     sdk install java 17.0.9-tem"
        echo ""
        echo "  3. Download from Adoptium:"
        echo "     https://adoptium.net/temurin/releases/?version=17"
    else
        echo "On Linux:"
        echo "  sudo apt-get update && sudo apt-get install openjdk-17-jdk  # Debian/Ubuntu"
        echo "  sudo yum install java-17-openjdk-devel  # RHEL/CentOS"
    fi
    echo ""
    echo "After installing Java 17, run this script again."
    exit 1
fi

echo "✅ Java found at: $JAVA_HOME_DETECTED"

# Try to get Java version
if [ -x "$JAVA_HOME_DETECTED/bin/java" ]; then
    JAVA_VERSION=$("$JAVA_HOME_DETECTED/bin/java" -version 2>&1 | head -1)
    echo "   Java version: $JAVA_VERSION"
fi

# Get Python executable path
# First, check if we're in a virtual environment
if [ -n "$VIRTUAL_ENV" ]; then
    PYTHON_EXEC="$VIRTUAL_ENV/bin/python"
elif [ -f ".venv/bin/python" ]; then
    # Check for common virtual environment location
    PYTHON_EXEC="$(pwd)/.venv/bin/python"
else
    # Fall back to system Python
    PYTHON_EXEC=$(which python 2>/dev/null || which python3 2>/dev/null)
    if [ -z "$PYTHON_EXEC" ]; then
        PYTHON_EXEC="python"
    fi
fi

echo "   Using Python: $PYTHON_EXEC"

# Create .env file
echo ""
echo "📍 Creating .env file..."
cat > .env << EOF
# Java Configuration
export JAVA_HOME="$JAVA_HOME_DETECTED"
export PATH="\$JAVA_HOME/bin:\$PATH"

# Spark Configuration
export SPARK_LOCAL_IP="127.0.0.1"
export PYSPARK_PYTHON="$PYTHON_EXEC"
export PYSPARK_DRIVER_PYTHON="$PYTHON_EXEC"

# Suppress Spark warnings
export SPARK_SUBMIT_OPTS="-Dlog4j.logLevel=ERROR"
EOF

echo "✅ .env file created"

# Source the environment
source .env

# Verify Java is accessible
echo ""
echo "📍 Verifying Java setup..."
if "$JAVA_HOME/bin/java" -version >/dev/null 2>&1; then
    echo "✅ Java is properly configured"
else
    echo "⚠️  Warning: Java might not be properly configured"
fi

echo ""
echo "🎯 To use these environment variables in your shell:"
echo "   source .env"
echo ""
echo "🎯 To run tests with the correct environment:"
echo "   make test-cov"
echo ""
echo "✅ Test environment setup complete!"
