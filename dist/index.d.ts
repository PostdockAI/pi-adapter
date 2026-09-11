type PiContext = {
    cwd?: string;
    sessionManager?: {
        getSessionFile?: () => string | Promise<string>;
    };
    isStreaming?: boolean | (() => boolean);
    ui?: {
        notify?: (message: string, level?: string) => void;
    };
};
type PiExtensionApi = {
    sendUserMessage: (content: string, options?: {
        deliverAs?: "steer" | "followUp";
    }) => void;
    registerCommand: (name: string, options: {
        description: string;
        handler: (args: string, ctx: PiContext) => Promise<void>;
    }) => void;
    registerTool: (tool: {
        name: string;
        label: string;
        description: string;
        parameters: unknown;
        promptSnippet?: string;
        promptGuidelines?: string[];
        execute: (toolCallId: string, params: {
            to: string;
            body: string;
            message_id?: string;
        }, signal: AbortSignal | undefined, onUpdate: unknown, ctx: PiContext) => Promise<{
            content: Array<{
                type: "text";
                text: string;
            }>;
            details: Record<string, unknown>;
        }>;
    }) => void;
    on: (event: string, handler: (event: unknown, ctx: PiContext) => Promise<void> | void) => void;
};
export default function registerMyagent(pi: PiExtensionApi): void;
export {};
