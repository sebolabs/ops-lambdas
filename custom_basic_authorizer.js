var validUsername = process.env.USERNAME;
var validPassword = process.env.PASSWORD;

exports.handler = (event, context, callback) => {
  console.log(JSON.stringify(event));
  var token = event.authorizationToken; // ex. Basic ZGVtbzpkZW1v

  if (!token) {
    console.log('[ERR] No authorization token provided');
  } else {
    console.log('[DEBUG] Auth token: '+token);

    var creds = new Buffer(token.split(' ')[1], 'base64').toString(); // ex. user:pass
    var authUsername = creds.split(':')[0];
    var authPassword = creds.split(':')[1];

    if (authUsername == validUsername && authPassword == validPassword) {
      console.log('[OK] Authorized user: '+ authUsername);
      context.succeed(generateIAMPolicy(authUsername, 'Allow', event.methodArn));
      return;
    }
  }

  context.fail('Unauthorized');
};

generateIAMPolicy = (principalId, effect, resource) => {
  var authResponse = {};
  authResponse.principalId = principalId;
  
  if (effect && resource) {
    var temp = resource.split(':'); // ex. arn:aws:execute-api:eu-west-1:001122334455:zf1w7sce4b/prod/GET/
    var region = temp[3];
    var accountId = temp[4];
    var restApiId = temp[5].split('/')[0];
    var stage = temp[5].split('/')[1];

    var policyDocument = {};
    policyDocument.Version = '2012-10-17';
    policyDocument.Statement = [];
    var statement0 = {};
    statement0.Action = 'execute-api:Invoke';
    statement0.Effect = effect;
    statement0.Resource = 'arn:aws:execute-api:'+region+':'+accountId+':'+restApiId+'/'+stage+'/*/*';
    policyDocument.Statement[0] = statement0;
    authResponse.policyDocument = policyDocument;
  }

  console.log(JSON.stringify(authResponse));
  return authResponse;
}
