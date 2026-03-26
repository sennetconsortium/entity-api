import {
  EC2Client,
  AuthorizeSecurityGroupIngressCommand,
  RevokeSecurityGroupIngressCommand
} from "@aws-sdk/client-ec2";

const {
  AWS_REGION,
  SECURITY_GROUP_ID,
  PORT,
  RUNNER_IP,
  MODE
} = process.env;

if (!AWS_REGION || !SECURITY_GROUP_ID || !PORT || !RUNNER_IP || !MODE) {
  throw new Error("Missing required environment variables");
}

const cidr = `${RUNNER_IP}/32`;
const port = Number(PORT);

const client = new EC2Client({ region: AWS_REGION });

const params = {
  GroupId: SECURITY_GROUP_ID,
  IpPermissions: [
    {
      IpProtocol: "tcp",
      FromPort: port,
      ToPort: port,
      IpRanges: [
        {
          CidrIp: cidr,
          Description: "GitHub Actions runner (temporary)"
        }
      ]
    }
  ]
};

try {
  if (MODE === "authorize") {
    console.log(`🔓 Authorizing ${cidr} on port ${port}`);
    await client.send(new AuthorizeSecurityGroupIngressCommand(params));
    console.log("✅ Runner IP added");
  } else if (MODE === "revoke") {
    console.log(`🔐 Revoking ${cidr} on port ${port}`);
    await client.send(new RevokeSecurityGroupIngressCommand(params));
    console.log("✅ Runner IP removed");
  } else {
    throw new Error(`Invalid MODE: ${MODE}`);
  }
} catch (error) {
  if (error.name === "InvalidPermission.Duplicate") {
    console.log("ℹ️ Rule already exists");
  } else if (error.name === "InvalidPermission.NotFound") {
    console.log("ℹ️ Rule already removed");
  } else {
    console.error("❌ AWS SG operation failed");
    throw error;
  }
}
